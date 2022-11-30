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
        Mutex,
        Condvar,
    },
    marker::{
        PhantomData,
    },
    time::{
        Instant,
        Duration,
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
    workers: Vec<thread::JoinHandle<()>>,
    shutdown: Arc<Shutdown>,
}

struct Shutdown {
    mutex: Mutex<bool>,
    condvar: Condvar,
}

pub struct Builder {
    worker_threads: Option<usize>,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
pub struct Counters {
    pub spawn_touch_tag_collisions: atomic::AtomicUsize,
}

#[derive(Debug, Default)]
pub struct Stats {
    pub acquire_job_time: Duration,
    pub acquire_job_count: usize,
    pub acquire_job_backoff_time: Duration,
    pub acquire_job_backoff_count: usize,
    pub acquire_job_thread_park_time: Duration,
    pub acquire_job_thread_park_count: usize,
    pub acquire_job_seg_queue_pop_time: Duration,
    pub acquire_job_seg_queue_pop_count: usize,
    pub job_run_time: Duration,
    pub job_run_count: usize,
    pub counters: Arc<Counters>,
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

        let counters = Arc::new(Counters::default());
        let inner: Arc<inner::Inner<J>> =
            Arc::new(inner::Inner::new(worker_threads, counters.clone())?);

        let mut maybe_error = Ok(());
        let mut workers = Vec::with_capacity(worker_threads);
        let workers_sync: Arc<(Mutex<Option<Arc<Vec<_>>>>, Condvar)> =
            Arc::new((Mutex::new(None), Condvar::new()));
        for worker_index in 0 .. worker_threads {
            let inner = inner.clone();
            let workers_sync = workers_sync.clone();
            let counters = counters.clone();
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

                    let mut worker_thread_pool = EdeltraudLocal {
                        inner_ref: &inner,
                        threads_ref: &threads,
                        stats: Stats { counters, ..Default::default() },
                    };
                    while let Some(job) = inner.acquire_job(worker_index, &mut worker_thread_pool.stats) {
                        let now = Instant::now();
                        job.run(&worker_thread_pool);
                        worker_thread_pool.stats.job_run_time += now.elapsed();
                        worker_thread_pool.stats.job_run_count += 1;
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
            workers,
            shutdown: Arc::new(Shutdown {
                mutex: Mutex::new(false),
                condvar: Condvar::new(),
            }),
        };

        maybe_error?;
        log::debug!("Edeltraud::new() success with {} workers", thread_pool.threads.len());
        Ok(thread_pool)
    }
}

impl<J> Edeltraud<J> where J: Job {
    pub fn handle(&self) -> EdeltraudHandle<J> {
        EdeltraudHandle {
            inner: self.inner.clone(),
            threads: self.threads.clone(),
        }
    }

    pub fn shutdown_timeout(self, timeout: Duration) {
        self.inner.force_terminate(&self.threads);
        if let Ok(mut lock) = self.shutdown.mutex.lock() {
            let mut current_timeout = timeout;
            let now = Instant::now();
            while !*lock {
                match self.shutdown.condvar.wait_timeout(lock, current_timeout) {
                    Ok((next_lock, wait_timeout_result)) => {
                        lock = next_lock;
                        if wait_timeout_result.timed_out() {
                            log::info!("shutdown wait timed out");
                            break;
                        }
                        current_timeout -= now.elapsed();
                    },
                    Err(..) => {
                        log::error!("failed to wait on shutdown condvar, terminating immediately");
                        break;
                    },
                }
            }
        } else {
            log::error!("failed to lock shutdown mutex, terminating immediately");
        }
    }
}

impl<J> Drop for Edeltraud<J> where J: Job {
    fn drop(&mut self) {
        log::debug!("Edeltraud::drop() invoked");
        self.inner.force_terminate(&self.threads);
        for join_handle in self.workers.drain(..) {
            join_handle.join().ok();
        }
        if let Ok(mut lock) = self.shutdown.mutex.lock() {
            *lock = true;
            self.shutdown.condvar.notify_all();
        } else {
            log::error!("failed to lock shutdown mutex on drop");
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

pub struct EdeltraudHandle<J> {
    inner: Arc<inner::Inner<J>>,
    threads: Arc<Vec<thread::Thread>>,
}

impl<J> Clone for EdeltraudHandle<J> where J: Job {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            threads: self.threads.clone(),
        }
    }
}

impl<J> ThreadPool<J> for EdeltraudHandle<J> where J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job {
        self.inner.spawn(job, &self.threads)
    }
}

struct EdeltraudLocal<'a, J> {
    inner_ref: &'a inner::Inner<J>,
    threads_ref: &'a [thread::Thread],
    stats: Stats,
}

impl<'a, J> Drop for EdeltraudLocal<'a, J> {
    fn drop(&mut self) {
        self.inner_ref.force_terminate(self.threads_ref);
        log::info!("EdeltraudLocal::drop on {:?}, stats: {:?}", thread::current(), self.stats);
    }
}

impl<'a, J> ThreadPool<J> for EdeltraudLocal<'a, J> where J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job {
        self.inner_ref.spawn(job, self.threads_ref)
    }
}

impl<'a, P, J> ThreadPool<J> for &'a P where P: ThreadPool<J>, J: Job {
    fn spawn(&self, job: J) -> Result<(), SpawnError> where J: Job {
        (*self).spawn(job)
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
