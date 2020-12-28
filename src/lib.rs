use std::{
    io,
    thread,
};

use crossbeam_channel as channel;

mod common;
mod slave;
mod dispatcher;

#[cfg(test)]
mod tests;

pub struct Edeltraud<T, R> {
    dispatcher_tx: channel::Sender<common::Event<T, R>>,
}

pub trait Job: Send + 'static {
    type Output;

    fn run(self) -> Self::Output;
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

    pub fn build<T, R>(&mut self) -> Result<Edeltraud<T, R>, BuildError>
    where T: Job<Output = R> + Send + 'static,
          R: Send + 'static,
    {
        let worker_threads = self.worker_threads
            .unwrap_or_else(|| num_cpus::get());

        let (dispatcher_tx, dispatcher_rx) = channel::unbounded();
        let mut slaves = Vec::with_capacity(worker_threads);

        for slave_index in 0 .. worker_threads {
            let (slave_tx, slave_rx) = channel::bounded(0);
            slaves.push(slave_tx);
            let dispatcher_tx = dispatcher_tx.clone();
            thread::Builder::new()
                .name(format!("edeltraud worker {}", slave_index))
                .spawn(move || slave::run(dispatcher_tx, slave_rx, slave_index))
                .map_err(BuildError::WorkerSpawn)?;
        }

        thread::Builder::new()
            .name("edeltraud dispatcher".to_string())
            .spawn(move || dispatcher::run(dispatcher_rx, slaves))
            .map_err(BuildError::DispatcherSpawn)?;

        Ok(Edeltraud { dispatcher_tx, })
    }
}

#[derive(Debug)]
pub enum BuildError {
    DispatcherSpawn(io::Error),
    WorkerSpawn(io::Error),
}

#[derive(Debug)]
pub enum SpawnError {
    ThreadPoolGone,
}

impl<T, R> Edeltraud<T, R> {
    pub async fn spawn<J>(&self, job: J) -> Result<R, SpawnError> where J: Job, T: From<J>, R: From<J::Output>,
    {
        use futures::channel::oneshot;

        let (reply_tx, reply_rx) = oneshot::channel();
        let task = common::Task {
            task: job.into(),
            reply_tx,
        };
        self.dispatcher_tx.send(common::Event::IncomingTask(task))
            .map_err(|_send_error| SpawnError::ThreadPoolGone)?;
        let output = reply_rx.await
            .map_err(|oneshot::Canceled| SpawnError::ThreadPoolGone)?;
        Ok(output.into())
    }
}

impl<T, R> Clone for Edeltraud<T, R> {
    fn clone(&self) -> Self {
        Self {
            dispatcher_tx: self.dispatcher_tx.clone(),
        }
    }
}
