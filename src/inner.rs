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
    pub state: Mutex<InnerState<J>>,
    pub condvar: Condvar,
}

pub enum InnerState<J> {
    Active(InnerStateActive<J>),
    Terminated,
}

pub struct InnerStateActive<J> {
    jobs_queue: Vec<J>,
}

impl<J> Inner<J> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(InnerState::Active(InnerStateActive { jobs_queue: Vec::new(), })),
            condvar: Condvar::new(),
        }
    }

    pub fn spawn(&self, job: J) -> Result<(), SpawnError> {
        let mut locked_state = self.state.lock()
            .map_err(|_| SpawnError::MutexLock)?;
        match &mut *locked_state {
            InnerState::Active(InnerStateActive { jobs_queue, }) => {
                jobs_queue.push(job);
                self.condvar.notify_one();
                Ok(())
            },
            InnerState::Terminated =>
                Err(SpawnError::MutexLock),
        }
    }

    pub fn acquire_job(&self) -> Option<J> {
        match self.state.lock() {
            Ok(mut locked_state) =>
                loop {
                    match &mut *locked_state {
                        InnerState::Active(InnerStateActive { jobs_queue, }) =>
                            if let Some(job) = jobs_queue.pop() {
                                return Some(job);
                            },
                        InnerState::Terminated =>
                            return None,
                    }
                    if let Ok(next_locked_state) = self.condvar.wait(locked_state) {
                        locked_state = next_locked_state;
                    } else {
                        return None;
                    }
                },
            Err(..) =>
                None,
        }
    }
}
