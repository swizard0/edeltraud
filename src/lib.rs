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
        Mutex,
        Condvar,
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
    fn run<P>(self, thread_pool: &P) where P: ThreadPool<Self>;
}

pub trait Computation: Sized + Send + 'static {
    type Output;

    fn run(self) -> Self::Output;
}

pub struct Edeltraud<J> where J: Job {
    inner: Arc<inner::Inner<J>>,
    threads: Arc<Vec<thread::Thread>>,
    workers: Option<Arc<Vec<thread::JoinHandle<()>>>>,
}

pub struct Builder {
    worker_threads: Option<usize>,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
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
            .unwrap_or_else(num_cpus::get);
        if worker_threads == 0 {
            return Err(BuildError::ZeroWorkerThreadsCount);
        }

        let inner: Arc<inner::Inner<J>> =
            Arc::new(inner::Inner::new(worker_threads)?);

        let mut maybe_error = Ok(());
        let mut workers = Vec::with_capacity(worker_threads);
        let workers_sync: Arc<(Mutex<Option<Arc<Vec<_>>>>, Condvar)> =
            Arc::new((Mutex::new(None), Condvar::new()));
        for worker_index in 0 .. worker_threads {
            let inner = inner.clone();
            let workers_sync = workers_sync.clone();
            let maybe_join_handle = thread::Builder::new()
                .name(format!("edeltraud worker {}", worker_index))
                .spawn(move || {
                    let threads = 'outer: loop {
                        let Ok(mut workers_sync_lock) =
                            workers_sync.0.lock() else { return; };
                        loop {
                            if let Some(threads) = workers_sync_lock.as_ref() {
                                break 'outer threads.clone();
                            }
                            let Ok(next_workers_sync_lock) =
                                workers_sync.1.wait(workers_sync_lock) else { return; };
                            workers_sync_lock = next_workers_sync_lock;
                            if inner.is_terminated() {
                                return;
                            }
                        }
                    };

                    let worker_thread_pool = LocalEdeltraud {
                        inner_ref: &inner,
                        threads_ref: &threads,
                    };
                    while let Some(job) = inner.acquire_job(worker_index) {
                        job.run(&worker_thread_pool);
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

        let threads: Arc<Vec<_>> = Arc::new(
            workers.iter()
                .map(|handle| handle.thread().clone())
                .collect(),
        );
        if let Ok(mut workers_sync_lock) = workers_sync.0.lock() {
            *workers_sync_lock = Some(threads.clone());
            workers_sync.1.notify_all();
        }

        let thread_pool = Edeltraud {
            inner,
            threads,
            workers: Some(Arc::new(workers)),
        };

        maybe_error?;
        Ok(thread_pool)
    }
}

impl<J> Drop for Edeltraud<J> where J: Job {
    fn drop(&mut self) {
        if let Some(workers_arc) = self.workers.take() {
            self.inner.force_terminate(&self.threads);
            if let Ok(workers) = Arc::try_unwrap(workers_arc) {
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
    TooBigWorkerThreadsCount,
    WorkerSpawn(io::Error),
}

#[derive(Debug)]
pub enum SpawnError {
    ThreadPoolGone,
    BucketMutexPoisoned,
}

pub trait ThreadPool<J> {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job;
}

pub struct AsyncJob<G> where G: Computation {
    computation: G,
    result_tx: oneshot::Sender<G::Output>,
}

pub fn job<P, J, G>(thread_pool: &P, job: G) -> Result<(), SpawnError>
where P: ThreadPool<J>,
      J: Job + From<G>,
{
    thread_pool.spawn(job.into())
}

pub fn job_async<P, J, G>(thread_pool: &P, computation: G) -> Result<AsyncResult<G::Output>, SpawnError>
where P: ThreadPool<J>,
      J: Job + From<AsyncJob<G>>,
      G: Computation,
{
    let (result_tx, result_rx) = oneshot::channel();
    let async_job = AsyncJob { computation, result_tx, };
    thread_pool.spawn(async_job.into())?;

    Ok(AsyncResult { result_rx, })
}

impl<G> Job for AsyncJob<G> where G: Computation, G::Output: Send {
    fn run<P>(self, _thread_pool: &P) where P: ThreadPool<Self> {
        let output = self.computation.run();
        if let Err(_send_error) = self.result_tx.send(output) {
            log::warn!("async result channel dropped before job is finished");
        }
    }
}

impl<J> Clone for Edeltraud<J> where J: Job {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            threads: self.threads.clone(),
            workers: self.workers.clone(),
        }
    }
}

impl<J> ThreadPool<J> for Edeltraud<J> where J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job {
        self.inner.spawn(job, &self.threads)
    }
}

impl<'a, P, J> ThreadPool<J> for &'a P where P: ThreadPool<J>, J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job {
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
      J: Job + From<G>,
      G: Job,
{
    fn spawn(&self, job: G) -> Result<(), SpawnError> {
        self.thread_pool.spawn(job.into())
    }
}

struct LocalEdeltraud<'a, J> {
    inner_ref: &'a inner::Inner<J>,
    threads_ref: &'a [thread::Thread],
}

impl<'a, J> Drop for LocalEdeltraud<'a, J> {
    fn drop(&mut self) {
        self.inner_ref.force_terminate(self.threads_ref);
    }
}

impl<'a, J> ThreadPool<J> for LocalEdeltraud<'a, J> where J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job {
        self.inner_ref.spawn(job, self.threads_ref)
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
