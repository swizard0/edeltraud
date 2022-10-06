use std::{
    sync::{
        atomic,
    },
    thread,
};

use crate::{
    SpawnError,
};

pub struct Inner<J> {
    injector: crossbeam::deque::Injector<J>,
    waiting_queue: Vec<atomic::AtomicBool>,
    pub is_terminated: atomic::AtomicBool,
}

impl<J> Inner<J> {
    pub fn new(workers_count: usize) -> Self {
        Self {
            injector: crossbeam::deque::Injector::new(),
            waiting_queue: (0 .. workers_count)
                .map(|_| atomic::AtomicBool::new(false))
                .collect(),
            is_terminated: atomic::AtomicBool::new(false),
        }
    }

    pub fn spawn(&self, job: J, maybe_workers: Option<&[thread::JoinHandle<()>]>) -> Result<(), SpawnError> {
        if self.is_terminated.load(atomic::Ordering::SeqCst) {
            return Err(SpawnError::ThreadPoolGone);
        }
        self.injector.push(job);
        if let Some(workers) = maybe_workers {
            for (join_handle, waiting_flag) in workers.iter().zip(self.waiting_queue.iter()) {
                if waiting_flag.swap(false, atomic::Ordering::SeqCst) {
                    join_handle.thread().unpark();
                }
            }
        }

        Ok(())
    }

    pub fn acquire_job(
        &self,
        worker_index: usize,
        sched_worker: &crossbeam::deque::Worker<J>,
        sched_stealers: &[crossbeam::deque::Stealer<J>],
    )
        -> Option<J>
    {
        let backoff = crossbeam::utils::Backoff::new();
        let mut steal = crossbeam::deque::Steal::Retry;

        self.waiting_queue[worker_index].store(true, atomic::Ordering::SeqCst);
        'outer: loop {
            if self.is_terminated.load(atomic::Ordering::SeqCst) {
                return None;
            }

            match steal {
                crossbeam::deque::Steal::Empty => {
                    // nothing to do, sleeping
                    if backoff.is_completed() {
                        loop {
                            thread::park();
                            if !self.waiting_queue[worker_index].swap(true, atomic::Ordering::SeqCst) ||
                                self.is_terminated.load(atomic::Ordering::SeqCst)
                            {
                                break;
                            }
                        }
                    } else {
                        backoff.snooze();
                    }
                    steal = crossbeam::deque::Steal::Retry;
                    continue 'outer;
                },
                crossbeam::deque::Steal::Success(job) => {
                    self.waiting_queue[worker_index].store(false, atomic::Ordering::SeqCst);
                    return Some(job);
                },
                crossbeam::deque::Steal::Retry =>
                    (),
            }

            // first try to acquire a job from the local queue
            if let Some(job) = sched_worker.pop() {
                steal = crossbeam::deque::Steal::Success(job);
                continue 'outer;
            }

            steal = crossbeam::deque::Steal::Empty;

            // next try to steal a batch from the injector
            match self.injector.steal_batch_and_pop(sched_worker) {
                crossbeam::deque::Steal::Empty =>
                    (),
                crossbeam::deque::Steal::Success(job) => {
                    steal = crossbeam::deque::Steal::Success(job);
                    continue 'outer;
                }
                crossbeam::deque::Steal::Retry =>
                    steal = crossbeam::deque::Steal::Retry,
            }

            // and finally try to steal a batch from another thread
            for sched_stealer in sched_stealers {
                match sched_stealer.steal_batch_and_pop(sched_worker) {
                    crossbeam::deque::Steal::Empty =>
                        (),
                    crossbeam::deque::Steal::Success(job) => {
                        steal = crossbeam::deque::Steal::Success(job);
                        continue 'outer;
                    }
                    crossbeam::deque::Steal::Retry =>
                        steal = crossbeam::deque::Steal::Retry,
                }
            }
        }
    }
}
