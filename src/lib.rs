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
        atomic,
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
    fn run<P, J>(self, thread_pool: &P) where P: ThreadPool<J>, J: Job + From<Self>;
}

pub trait Computation: Sized + Send + 'static {
    type Output;

    fn run(self) -> Self::Output;
}

pub struct Edeltraud<J> where J: Job {
    inner: Arc<inner::Inner<J>>,
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

        let sched_workers: Vec<_> = (0 .. worker_threads)
            .map(|_| crossbeam::deque::Worker::new_lifo())
            .collect();
        let mut sched_stealers = Vec::with_capacity(worker_threads);
        for worker_index in 0 .. worker_threads {
            let stealers: Vec<_> = (0 .. worker_threads)
                .flat_map(|stealer_index| if worker_index == stealer_index {
                    None
                } else {
                    Some(sched_workers[stealer_index].stealer())
                })
                .collect();
            sched_stealers.push(stealers);
        }

        let inner: Arc<inner::Inner<J>> =
            Arc::new(inner::Inner::new(worker_threads));

        let mut maybe_error = Ok(());
        let mut workers = Vec::with_capacity(worker_threads);
        let sched_iter = sched_workers.into_iter()
            .zip(sched_stealers.into_iter())
            .enumerate();
        for (worker_index, (sched_worker, sched_stealers)) in sched_iter {
            let inner = inner.clone();
            let maybe_join_handle = thread::Builder::new()
                .name(format!("edeltraud worker {}", worker_index))
                .spawn(move || {
                    let _drop_bomb = DropBomp { is_terminated: &inner.is_terminated, };
                    let worker_thread_pool = LocalEdeltraud { sched_worker: &sched_worker };
                    while let Some(job) = inner.acquire_job(worker_index, &sched_worker, &sched_stealers) {
                        job.run(&worker_thread_pool);
                    }

                    struct DropBomp<'a> {
                        is_terminated: &'a atomic::AtomicBool,
                    }

                    impl<'a> Drop for DropBomp<'a> {
                        fn drop(&mut self) {
                            self.is_terminated.store(true, atomic::Ordering::SeqCst);
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
                self.inner.is_terminated.store(true, atomic::Ordering::SeqCst);
                for join_handle in workers {
                    join_handle.thread().unpark();
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
    ThreadPoolGone,
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
    fn run<P, J>(self, _thread_pool: &P) where P: ThreadPool<J>, J: Job + From<Self> {
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
            workers: self.workers.clone(),
        }
    }
}

impl<J> ThreadPool<J> for Edeltraud<J> where J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job {
        self.inner.spawn(job, self.workers.as_ref().map(|v| &***v))
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
    sched_worker: &'a crossbeam::deque::Worker<J>,
}

impl<'a, J> ThreadPool<J> for LocalEdeltraud<'a, J> where J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job {
        self.sched_worker.push(job);
        Ok(())
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
