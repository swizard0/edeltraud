use std::{
    thread,
    sync::{
        atomic,
        Arc,
        Mutex,
        TryLockError,
    },
    time::{
        Instant,
    },
};

use crate::{
    Stats,
    Counters,
    SpawnError,
    BuildError,
};

struct Bucket<J> {
    slot: Mutex<BucketSlot<J>>,
    touch_tag: TouchTag,
    taken_by: atomic::AtomicUsize,
}

struct BucketSlot<J> {
    jobs_queue: Vec<J>,
}

impl<J> Bucket<J> {
    fn notify_waiting_worker(&self, threads: &[thread::Thread]) {
        let taken_by = self.taken_by.load(atomic::Ordering::SeqCst);
        if taken_by > 0 {
            let worker_index = taken_by - 1;
            threads[worker_index].unpark();
        }
    }
}

struct TouchTag {
    tag: atomic::AtomicU64,
}

impl Default for TouchTag {
    fn default() -> TouchTag {
        TouchTag {
            tag: atomic::AtomicU64::new(0),
        }
    }
}

impl TouchTag {
    const JOBS_COUNT_MASK: u64 = u32::MAX as u64;
    const COLLISION_MASK: u64 = Self::JOBS_COUNT_MASK + 1;

    fn load(&self) -> u64 {
        self.tag.load(atomic::Ordering::SeqCst)
    }

    fn try_set(&self, prev_tag: u64, new_tag: u64) -> bool {
        self.tag
            .compare_exchange(
                prev_tag,
                new_tag,
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            )
            .is_ok()
    }

    fn inc_jobs_count(&self) {
        self.tag.fetch_add(1, atomic::Ordering::SeqCst);
    }

    fn mark_collision(&self) {
        self.tag.fetch_or(Self::COLLISION_MASK, atomic::Ordering::SeqCst);
    }

    fn decompose(tag: u64) -> (bool, usize) {
        (tag & Self::COLLISION_MASK != 0, (tag & Self::JOBS_COUNT_MASK) as usize)
    }

    fn compose(collision_flag: bool, jobs_count: usize) -> u64 {
        let mut tag = jobs_count as u64;
        if collision_flag {
            tag |= Self::COLLISION_MASK;
        }
        tag
    }
}

impl<J> Default for Bucket<J> {
    fn default() -> Self {
        Self {
            slot: Mutex::new(BucketSlot {
                jobs_queue: Vec::new(),
            }),
            touch_tag: TouchTag::default(),
            taken_by: atomic::AtomicUsize::new(0),
        }
    }
}

pub struct Inner<J> {
    buckets: Vec<Bucket<J>>,
    spawn_index_counter: atomic::AtomicUsize,
    await_index_counter: atomic::AtomicUsize,
    is_terminated: atomic::AtomicBool,
    counters: Arc<Counters>,
}

impl<J> Inner<J> {
    pub(super) fn new(workers_count: usize, counters: Arc<Counters>) -> Result<Self, BuildError> {
        Ok(Self {
            buckets: (0 .. workers_count)
                .map(|_| Bucket::default())
                .collect(),
            spawn_index_counter: atomic::AtomicUsize::new(0),
            await_index_counter: atomic::AtomicUsize::new(0),
            is_terminated: atomic::AtomicBool::new(false),
            counters,
        })
    }

    pub(super) fn force_terminate(&self, threads: &[thread::Thread]) {
        self.is_terminated.store(true, atomic::Ordering::SeqCst);
        for thread in threads {
            thread.unpark();
        }
    }

    pub(super) fn spawn(&self, job: J, threads: &[thread::Thread]) -> Result<(), SpawnError> {
        loop {
            if self.is_terminated() {
                return Err(SpawnError::ThreadPoolGone);
            }

            let bucket_index = self.spawn_index_counter.fetch_add(1, atomic::Ordering::Relaxed) % self.buckets.len();
            let bucket = &self.buckets[bucket_index];
            match bucket.slot.try_lock() {
                Ok(mut slot) => {
                    slot.jobs_queue.push(job);
                    bucket.touch_tag.inc_jobs_count();
                    bucket.notify_waiting_worker(threads);
                    self.counters.spawn_mutex_lock_success.fetch_add(1, atomic::Ordering::Relaxed);
                    return Ok(());
                },
                Err(TryLockError::WouldBlock) => {
                    bucket.touch_tag.mark_collision();
                    bucket.notify_waiting_worker(threads);
                    self.counters.spawn_mutex_lock_fail.fetch_add(1, atomic::Ordering::Relaxed);
                },
                Err(TryLockError::Poisoned(..)) =>
                    return Err(SpawnError::BucketMutexPoisoned),
            }
        }
    }

    pub(super) fn acquire_job(&self, worker_index: usize, stats: &mut Stats) -> Option<J> {
        let now = Instant::now();
        let maybe_job = self.actually_acquire_job(worker_index, stats);
        stats.acquire_job_time += now.elapsed();
        stats.acquire_job_count += 1;
        maybe_job
    }

    fn actually_acquire_job(&self, worker_index: usize, stats: &mut Stats) -> Option<J> {
        'outer: loop {
            if self.is_terminated() {
                return None;
            }

            let bucket_index = self.await_index_counter.fetch_add(1, atomic::Ordering::Relaxed) % self.buckets.len();
            let bucket = &self.buckets[bucket_index];
            let taken_by_result = bucket.taken_by
                .compare_exchange(
                    0,
                    worker_index + 1,
                    atomic::Ordering::Acquire,
                    atomic::Ordering::Relaxed,
                );
            if let Err(..) = taken_by_result {
                // bucket is already taken, proceed to the next one
                continue 'outer;
            }

            loop {
                let old_touch_tag = bucket.touch_tag.load();
                let (collision_flag, jobs_count) = TouchTag::decompose(old_touch_tag);
                if jobs_count > 0 {
                    // got a job in `jobs_queue`, try to grab it
                    let new_touch_tag = TouchTag::compose(false, jobs_count - 1);
                    if !bucket.touch_tag.try_set(old_touch_tag, new_touch_tag) {
                        continue;
                    }
                    break;
                }
                if collision_flag {
                    // `spawn` collision occurred and no jobs in `jobs_queue`, skip this bucket
                    let new_touch_tag = TouchTag::compose(false, 0);
                    if !bucket.touch_tag.try_set(old_touch_tag, new_touch_tag) {
                        continue;
                    }

                    bucket.taken_by.store(0, atomic::Ordering::SeqCst);
                    continue 'outer;
                }

                // nothing interesting, wait for something to occur
                let now = Instant::now();
                thread::park();
                stats.acquire_job_thread_park_time += now.elapsed();
                stats.acquire_job_thread_park_count += 1;
                if self.is_terminated() {
                    return None;
                }
            }

            let now = Instant::now();
            let mut slot = bucket.slot.lock().ok()?;
            stats.acquire_job_mutex_lock_time += now.elapsed();
            stats.acquire_job_mutex_lock_count += 1;
            let job = slot.jobs_queue.pop().unwrap();
            bucket.taken_by.store(0, atomic::Ordering::SeqCst);
            return Some(job)
        }
    }

    pub(super) fn is_terminated(&self) -> bool {
        self.is_terminated.load(atomic::Ordering::Relaxed)
    }
}
