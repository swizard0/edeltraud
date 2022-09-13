use std::{
    sync::{
        atomic,
    },
    thread,
};

pub enum Command<J> {
    Job(J),
    Terminate,
}

pub struct Inner<J> {
    injector: crossbeam::deque::Injector<Command<J>>,
    waiting_queue: Vec<atomic::AtomicBool>,
}

impl<J> Inner<J> {
    pub fn new(workers_count: usize) -> Self {
        Self {
            injector: crossbeam::deque::Injector::new(),
            waiting_queue: (0 .. workers_count)
                .map(|_| atomic::AtomicBool::new(false))
                .collect(),
        }
    }

    pub fn command(&self, command: Command<J>, maybe_workers: Option<&[thread::JoinHandle<()>]>) {
        self.injector.push(command);
        if let Some(workers) = maybe_workers {
            for (join_handle, waiting_flag) in workers.iter().zip(self.waiting_queue.iter()) {
                if waiting_flag.swap(false, atomic::Ordering::SeqCst) {
                    join_handle.thread().unpark();
                }
            }
        }
    }

    pub fn acquire_job(
        &self,
        worker_index: usize,
        sched_worker: &crossbeam::deque::Worker<Command<J>>,
        sched_stealers: &[crossbeam::deque::Stealer<Command<J>>],
    )
        -> Option<J>
    {
        let backoff = crossbeam::utils::Backoff::new();
        let mut steal = crossbeam::deque::Steal::Retry;
        'outer: loop {
            match steal {
                crossbeam::deque::Steal::Empty => {
                    // nothing to do, sleeping
                    if backoff.is_completed() {
                        self.waiting_queue[worker_index].store(true, atomic::Ordering::SeqCst);
                        thread::park();
                        self.waiting_queue[worker_index].store(false, atomic::Ordering::SeqCst);
                    } else {
                        backoff.snooze();
                    }
                    steal = crossbeam::deque::Steal::Retry;
                    continue 'outer;
                },
                crossbeam::deque::Steal::Success(Command::Job(job)) =>
                    return Some(job),
                crossbeam::deque::Steal::Success(Command::Terminate) =>
                    return None,
                crossbeam::deque::Steal::Retry =>
                    (),
            }

            // first try to acquire a job from the local queue
            if let Some(command) = sched_worker.pop() {
                steal = crossbeam::deque::Steal::Success(command);
                continue 'outer;
            }

            steal = crossbeam::deque::Steal::Empty;

            // next try to steal a batch from the injector
            match self.injector.steal_batch_and_pop(sched_worker) {
                crossbeam::deque::Steal::Empty =>
                    (),
                crossbeam::deque::Steal::Success(command) => {
                    steal = crossbeam::deque::Steal::Success(command);
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
                    crossbeam::deque::Steal::Success(command) => {
                        steal = crossbeam::deque::Steal::Success(command);
                        continue 'outer;
                    }
                    crossbeam::deque::Steal::Retry =>
                        steal = crossbeam::deque::Steal::Retry,
                }
            }
        }
    }
}
