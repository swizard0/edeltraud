use std::{
    sync::{
        Mutex,
        Condvar,
    },
};

use crate::{
    SpawnError,
};

pub struct Inner<J> {
    jobs_queue: Mutex<Vec<J>>,
    condvar: Condvar,
}

impl<J> Inner<J> {
    pub fn new() -> Self {
        Self {
            jobs_queue: Mutex::new(Vec::new()),
            condvar: Condvar::new(),
        }
    }

    pub fn spawn(&self, job: J) -> Result<(), SpawnError> {
        let mut jobs_queue = self.jobs_queue.lock()
            .map_err(|_| SpawnError::MutexLock)?;
        jobs_queue.push(job);
        self.condvar.notify_one();
        Ok(())
    }

    pub fn acquire_job(&self) -> J {
        let mut jobs_queue = self.jobs_queue.lock().unwrap();
        loop {
            if let Some(job) = jobs_queue.pop() {
                return job;
            }
            jobs_queue = self.condvar.wait(jobs_queue).unwrap();
        }
    }
}
