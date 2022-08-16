use std::{
    sync::{
        Mutex,
        Condvar,
    },
};

use crate::{
    common::{
        Task,
    },
    Job,
    SpawnError,
};

pub struct Inner<T> where T: Job {
    tasks_queue: Mutex<Vec<Task<T>>>,
    condvar: Condvar,
}

impl<T> Inner<T> where T: Job {
    pub fn new() -> Self {
        Self {
            tasks_queue: Mutex::new(Vec::new()),
            condvar: Condvar::new(),
        }
    }

    pub fn spawn<J>(&self, task: Task<T>) -> Result<(), SpawnError> where J: Job, T: From<J> {
        let mut tasks_queue = self.tasks_queue.lock()
            .map_err(|_| SpawnError::MutexLock)?;
        tasks_queue.push(task);
        self.condvar.notify_one();
        Ok(())
    }

    pub fn acquire_task(&self) -> Task<T> {
        let mut tasks_queue = self.tasks_queue.lock().unwrap();
        loop {
            if let Some(task) = tasks_queue.pop() {
                return task;
            }
            tasks_queue = self.condvar.wait(tasks_queue).unwrap();
        }
    }
}
