use std::{
    sync::{
        atomic,
        Mutex,
        Condvar,
    },
};

use crate::{
    SpawnError,
    BuildError,
};

struct Bucket<J> {
    slot: Mutex<BucketSlot<J>>,
    condvar: Condvar,
}

struct BucketSlot<J> {
    jobs_queue: Vec<J>,
    pending_await: bool,
}


impl<J> Default for Bucket<J> {
    fn default() -> Self {
        Self {
            slot: Mutex::new(BucketSlot {
                jobs_queue: Vec::new(),
                pending_await: false,
            }),
            condvar: Condvar::new(),
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

    pub fn force_terminate(&self) {
        self.is_terminated.store(true, atomic::Ordering::SeqCst);
        for bucket in &self.buckets {
            if let Ok(slot) = bucket.slot.lock() {
                if slot.pending_await {
                    bucket.condvar.notify_all();
                }
            }
        }
    }

    pub fn spawn(&self, job: J) -> Result<(), SpawnError> {
        let bucket_index = self.spawn_index_counter.fetch_add(1, atomic::Ordering::Relaxed) % self.buckets.len();
        let bucket = &self.buckets[bucket_index];
        let mut slot = bucket.slot.lock()
            .map_err(|_| SpawnError::BucketMutexPoisoned)?;
        slot.jobs_queue.push(job);
        if slot.pending_await {
            bucket.condvar.notify_one();
        }
        Ok(())
    }

    pub fn acquire_job(&self) -> Option<J> {
        if self.is_terminated() {
            return None;
        }

        let bucket_index = self.await_index_counter.fetch_add(1, atomic::Ordering::Relaxed) % self.buckets.len();
        let bucket = &self.buckets[bucket_index];
        let mut slot = bucket.slot.lock().ok()?;
        assert!(!slot.pending_await, "slot pending await detected while acquire_job: bucket_index = {bucket_index}, #buckets = {}", self.buckets.len());
        while slot.jobs_queue.is_empty() {
            slot.pending_await = true;
            slot = bucket.condvar.wait(slot).ok()?;
            slot.pending_await = false;
            if self.is_terminated() {
                return None;
            }
        }
        slot.jobs_queue.pop()
    }

    fn is_terminated(&self) -> bool {
        self.is_terminated.load(atomic::Ordering::Relaxed)
    }
}
