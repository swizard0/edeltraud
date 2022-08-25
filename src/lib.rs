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
    marker::{
        PhantomData,
    },
};

use futures::{
    channel::{
        oneshot,
    },
    Future,
};

mod inner;

#[cfg(test)]
mod tests;

pub trait Job: Sized + Send + 'static {
    type Output: Send + 'static;

    fn run<P>(self, thread_pool: &P) -> Self::Output where P: ThreadPool<Self>;
}

pub struct Edeltraud<J> where J: Job {
    inner: Arc<inner::Inner<J>>,
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

    pub fn build<J>(&mut self) -> Result<Edeltraud<J>, BuildError> where J: Job {
        let worker_threads = self.worker_threads
            .unwrap_or_else(|| num_cpus::get());
        let thread_pool: Edeltraud<J> = Edeltraud {
            inner: Arc::new(inner::Inner::new()),
        };
        for slave_index in 0 .. worker_threads {
            let thread_pool = thread_pool.clone();
            thread::Builder::new()
                .name(format!("edeltraud worker {}", slave_index))
                .spawn(move || {
                    loop {
                        let job = thread_pool.inner.acquire_job();
                        job.run(&thread_pool);
                    }
                })
                .map_err(BuildError::WorkerSpawn)?;
        }
        Ok(thread_pool)
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

pub trait ThreadPool<J> {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job<Output = ()>;
}

impl<J> ThreadPool<J> for Edeltraud<J> where J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job<Output = ()> {
        self.inner.spawn(job)
    }
}

pub enum AsyncJob<G> where G: Job {
    Master {
        job: G,
        result_tx: oneshot::Sender<G::Output>,
    },
    Slave(G),
}

impl<G> From<G> for AsyncJob<G> where G: Job {
    fn from(job: G) -> AsyncJob<G> {
        AsyncJob::Slave(job)
    }
}

pub fn job_async<P, J, G>(thread_pool: &P, job: G) -> Result<AsyncResult<G::Output>, SpawnError>
where P: ThreadPool<J>,
      J: Job<Output = ()> + From<AsyncJob<G>>,
      G: Job,
{
    let (result_tx, result_rx) = oneshot::channel();
    let async_job = AsyncJob::Master { job, result_tx, };
    thread_pool.spawn(async_job.into())?;

    Ok(AsyncResult { result_rx, })
}

impl<G> Job for AsyncJob<G> where G: Job {
    type Output = ();

    fn run<P>(self, thread_pool: &P) -> Self::Output where P: ThreadPool<Self> {
        let thread_pool_job_map = EdeltraudJobMap::new(thread_pool);
        match self {
            AsyncJob::Master { job, result_tx, } => {
                let output = job.run(&thread_pool_job_map);
                if let Err(_send_error) = result_tx.send(output) {
                    log::warn!("async result channel dropped before job is finished");
                }
            },
            AsyncJob::Slave(job) => {
                job.run(&thread_pool_job_map);
            },
        }
    }
}

impl<J> Clone for Edeltraud<J> where J: Job {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct EdeltraudJobMap<'a, P, J, G> {
    thread_pool: &'a P,
    _marker: PhantomData<(J, G)>,
}

impl<'a, P, J, G> EdeltraudJobMap<'a, P, J, G> {
    pub fn new(thread_pool: &'a P) -> Self {
        Self { thread_pool, _marker: PhantomData, }
    }
}

impl<'a, P, J, G> ThreadPool<G> for EdeltraudJobMap<'a, P, J, G>
where P: ThreadPool<J>,
      J: Job<Output = ()> + From<G>,
      G: Job,
{
    fn spawn(&self, job: G) -> Result<(), SpawnError> where G: Job {
        self.thread_pool.spawn(job.into())
    }
}

pub struct AsyncResult<T> {
    result_rx: oneshot::Receiver<T>,
}

impl<T> Future for AsyncResult<T> {
    type Output = Result<T, SpawnError>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result_rx = Pin::new(&mut this.result_rx);
        result_rx.poll(cx)
            .map_err(|oneshot::Canceled| SpawnError::ThreadPoolGone)
    }
}
