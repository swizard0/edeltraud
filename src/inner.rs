use std::{
    thread,
    sync::{
        atomic,
        Mutex,
    },
};

use crate::{
    SpawnError,
    BuildError,
};

struct Bucket<J> {
    slot: Mutex<BucketSlot<J>>,
    jobs_count: atomic::AtomicUsize,
    taken_by: atomic::AtomicUsize,
}

struct BucketSlot<J> {
    jobs_queue: Vec<J>,
}

impl<J> Default for Bucket<J> {
    fn default() -> Self {
        Self {
            slot: Mutex::new(BucketSlot {
                jobs_queue: Vec::new(),
            }),
            jobs_count: atomic::AtomicUsize::new(0),
            taken_by: atomic::AtomicUsize::new(0),
        }
    }
}

pub struct Inner<J> {
    buckets: Vec<Bucket<J>>,
    spawn_index_counter: atomic::AtomicUsize,
    await_index_counter: atomic::AtomicUsize,
    is_terminated: atomic::AtomicBool,
}

impl<J> Inner<J> {
    pub fn new(workers_count: usize) -> Result<Self, BuildError> {
        Ok(Self {
            buckets: (0 .. workers_count)
                .map(|_| Bucket::default())
                .collect(),
            spawn_index_counter: atomic::AtomicUsize::new(0),
            await_index_counter: atomic::AtomicUsize::new(0),
            is_terminated: atomic::AtomicBool::new(false),
        })
    }

    pub fn force_terminate(&self, threads: &[thread::Thread]) {
        self.is_terminated.store(true, atomic::Ordering::SeqCst);
        for thread in threads {
            thread.unpark();
        }
    }

    pub fn spawn(&self, job: J, threads: &[thread::Thread]) -> Result<(), SpawnError> {
        let bucket_index = self.spawn_index_counter.fetch_add(1, atomic::Ordering::Relaxed) % self.buckets.len();
        let bucket = &self.buckets[bucket_index];
        let mut slot = bucket.slot.lock()
            .map_err(|_| SpawnError::BucketMutexPoisoned)?;
        slot.jobs_queue.push(job);
        bucket.jobs_count.fetch_add(1, atomic::Ordering::Relaxed);
        let taken_by = bucket.taken_by.load(atomic::Ordering::SeqCst);
        if taken_by > 0 {
            let worker_index = taken_by - 1;
            threads[worker_index].unpark();
        }
        Ok(())
    }

    pub fn acquire_job(&self, worker_index: usize) -> Option<J> {
        loop {
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
                continue;
            }

            while bucket.jobs_count.load(atomic::Ordering::SeqCst) == 0 {
                thread::park();
                if self.is_terminated() {
                    return None;
                }
            }

            let mut slot = bucket.slot.lock().ok()?;
            let job = slot.jobs_queue.pop().unwrap();
            bucket.jobs_count.fetch_sub(1, atomic::Ordering::SeqCst);
            bucket.taken_by.store(0, atomic::Ordering::SeqCst);
            return Some(job)
        }
    }

    pub fn is_terminated(&self) -> bool {
        self.is_terminated.load(atomic::Ordering::Relaxed)
    }
}
