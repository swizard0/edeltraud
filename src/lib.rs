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
    workers: Option<Arc<Vec<thread::JoinHandle<()>>>>,
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
        self.worker_threads = Some(value);
        self
    }

    pub fn build<J>(&mut self) -> Result<Edeltraud<J>, BuildError> where J: Job {
        let worker_threads = self.worker_threads
            .unwrap_or_else(|| num_cpus::get());
        if worker_threads == 0 {
            return Err(BuildError::ZeroWorkerThreadsCount);
        }

        let inner: Arc<inner::Inner<J>> = Arc::new(inner::Inner::new());
        let mut maybe_error = Ok(());
        let mut workers = Vec::with_capacity(worker_threads);
        for worker_index in 0 .. worker_threads {
            let inner = inner.clone();
            let maybe_join_handle = thread::Builder::new()
                .name(format!("edeltraud worker {}", worker_index))
                .spawn(move || {
                    let worker_thread_pool = Edeltraud { inner, workers: None, };
                    loop {
                        if let Some(job) = worker_thread_pool.inner.acquire_job() {
                            job.run(&worker_thread_pool);
                        } else {
                            break;
                        }
                    }
                })
                .map_err(BuildError::WorkerSpawn);
            match maybe_join_handle {
                Ok(join_handle) =>
                    workers.push(join_handle),
                Err(error) =>
                    maybe_error = Err(error),
            }
        }

        let thread_pool = Edeltraud {
            inner,
            workers: Some(Arc::new(workers)),
        };
        maybe_error?;
        Ok(thread_pool)
    }
}

impl<J> Drop for Edeltraud<J> where J: Job {
    fn drop(&mut self) {
        if let Some(workers_arc) = self.workers.take() {
            if let Ok(workers) = Arc::try_unwrap(workers_arc) {
                if let Ok(mut locked_state) = self.inner.state.lock() {
                    if let inner::InnerState::Active(..) = &*locked_state {
                        *locked_state = inner::InnerState::Terminated;
                        self.inner.condvar.notify_all();
                    }
                }
                for join_handle in workers {
                    join_handle.join().ok();
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum BuildError {
    ZeroWorkerThreadsCount,
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

pub struct AsyncJob<G> where G: Job {
    inner: AsyncJobInner<G>,
}

enum AsyncJobInner<G> where G: Job {
    Master {
        job: G,
        result_tx: oneshot::Sender<G::Output>,
    },
    Slave(G),
}

impl<G> From<G> for AsyncJob<G> where G: Job {
    fn from(job: G) -> AsyncJob<G> {
        AsyncJob { inner: AsyncJobInner::Slave(job), }
    }
}

pub fn job<P, J, G>(thread_pool: &P, job: G) -> Result<(), SpawnError>
where P: ThreadPool<J>,
      J: Job<Output = ()> + From<G>,
{
    thread_pool.spawn(job.into())
}

pub fn job_async<P, J, G>(thread_pool: &P, job: G) -> Result<AsyncResult<G::Output>, SpawnError>
where P: ThreadPool<J>,
      J: Job<Output = ()> + From<AsyncJob<G>>,
      G: Job,
{
    let (result_tx, result_rx) = oneshot::channel();
    let async_job = AsyncJob {
        inner: AsyncJobInner::Master { job, result_tx, },
    };
    thread_pool.spawn(async_job.into())?;

    Ok(AsyncResult { result_rx, })
}

impl<G> Job for AsyncJob<G> where G: Job {
    type Output = ();

    fn run<P>(self, thread_pool: &P) -> Self::Output where P: ThreadPool<Self> {
        let thread_pool_map = ThreadPoolMap::new(thread_pool);
        match self.inner {
            AsyncJobInner::Master { job, result_tx, } => {
                let output = job.run(&thread_pool_map);
                if let Err(_send_error) = result_tx.send(output) {
                    log::warn!("async result channel dropped before job is finished");
                }
            },
            AsyncJobInner::Slave(job) => {
                job.run(&thread_pool_map);
            },
        }
    }
}

impl<J> Clone for Edeltraud<J> where J: Job {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            workers: self.workers.clone(),
        }
    }
}

impl<J> ThreadPool<J> for Edeltraud<J> where J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job<Output = ()> {
        self.inner.spawn(job)
    }
}

impl<'a, P, J> ThreadPool<J> for &'a P where P: ThreadPool<J>, J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job<Output = ()> {
        (*self).spawn(job)
    }
}

pub struct ThreadPoolMap<P, J, G> {
    thread_pool: P,
    _marker: PhantomData<(J, G)>,
}

impl<P, J, G> ThreadPoolMap<P, J, G> {
    pub fn new(thread_pool: P) -> Self {
        Self { thread_pool, _marker: PhantomData, }
    }
}

impl<P, J, G> Clone for ThreadPoolMap<P, J, G> where P: Clone {
    fn clone(&self) -> Self {
        Self {
            thread_pool: self.thread_pool.clone(),
            _marker: PhantomData,
        }
    }
}

impl<P, J, G> ThreadPool<G> for ThreadPoolMap<P, J, G>
where P: ThreadPool<J>,
      J: Job<Output = ()> + From<G>,
      G: Job,
{
    fn spawn(&self, job: G) -> Result<(), SpawnError> where G: Job<Output = ()> {
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
