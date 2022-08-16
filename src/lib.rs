#![forbid(unsafe_code)]

use std::{
    io,
    thread,
    pin::{
        Pin,
    },
    task::{
        Poll,
    },
    sync::{
        Arc,
    },
};

use futures::{
    channel::{
        oneshot,
    },
    Future,
};

mod common;
mod slave;
mod inner;

#[cfg(test)]
mod tests;

pub trait Job: Send + 'static {
    type Output: Send + 'static;

    fn run(self) -> Self::Output;
}

pub struct Edeltraud<T> where T: Job {
    inner: Arc<inner::Inner<T>>,
}

pub struct Builder {
    worker_threads: Option<usize>,
}

impl Builder {
    pub fn new() -> Builder {
        Builder {
            worker_threads: None,
        }
    }

    pub fn worker_threads(&mut self, value: usize) -> &mut Self {
        assert!(value > 0, "Worker threads cannot be set to 0");
        self.worker_threads = Some(value);
        self
    }

    pub fn build<T>(&mut self) -> Result<Edeltraud<T>, BuildError> where T: Job {
        let worker_threads = self.worker_threads
            .unwrap_or_else(|| num_cpus::get());
        let inner = Arc::new(inner::Inner::new());
        for slave_index in 0 .. worker_threads {
            let inner = inner.clone();
            thread::Builder::new()
                .name(format!("edeltraud worker {}", slave_index))
                .spawn(move || slave::run(&inner, slave_index))
                .map_err(BuildError::WorkerSpawn)?;
        }
        Ok(Edeltraud { inner, })
    }
}

#[derive(Debug)]
pub enum BuildError {
    WorkerSpawn(io::Error),
}

#[derive(Debug)]
pub enum SpawnError {
    MutexLock,
    ThreadPoolGone,
}

impl<T> Edeltraud<T> where T: Job {
    pub fn spawn_noreply<J>(&self, job: J) -> Result<(), SpawnError> where J: Job<Output = ()>, T: From<J> {
        let task = common::Task {
            task: job.into(),
            task_reply: common::TaskReply::None,
        };
        self.inner.spawn(task)
    }

    pub fn spawn_handle<J>(&self, job: J) -> Result<Handle<T::Output>, SpawnError> where J: Job, T: From<J> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let task = common::Task {
            task: job.into(),
            task_reply: common::TaskReply::Oneshot { reply_tx, },
        };
        self.inner.spawn(task)?;
        Ok(Handle { reply_rx, })
    }

    pub async fn spawn<J>(&self, job: J) -> Result<T::Output, SpawnError> where J: Job, T: From<J> {
        let handle = self.spawn_handle(job)?;
        handle.await
    }
}

impl<T> Clone for Edeltraud<T> where T: Job {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct Handle<T> {
    reply_rx: oneshot::Receiver<T>,
}

impl<T> Future for Handle<T> {
    type Output = Result<T, SpawnError>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let reply_rx = Pin::new(&mut this.reply_rx);
        reply_rx.poll(cx)
            .map_err(|oneshot::Canceled| SpawnError::ThreadPoolGone)
    }
}
