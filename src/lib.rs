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

pub trait Job: Sized {
    fn run(self);
}

pub struct JobUnit<'a, J, G> {
    pub handle: &'a Handle<J>,
    pub job: G,
}

pub trait Computation: Sized {
    type Output;

    fn run(self) -> Self::Output;
}

pub struct Edeltraud<J> {
    inner: Arc<inner::Inner<J>>,
    threads: Arc<Vec<thread::Thread>>,
    workers: Vec<thread::JoinHandle<()>>,
    shutdown: Arc<Shutdown>,
}

pub struct Handle<J> {
    inner: Arc<inner::Inner<J>>,
    threads: Arc<Vec<thread::Thread>>,
}

impl<J> Clone for Handle<J> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            threads: self.threads.clone(),
        }
    }
}

impl<J> Handle<J> {
    fn spawn<G>(&self, job: G) -> Result<(), SpawnError> where J: From<G> {
        self.inner.spawn(job.into(), &self.threads)
    }
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
    pub spawn_total_count: atomic::AtomicUsize,
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
    pub acquire_job_taken_by_collisions: usize,
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

    pub fn build<J>(&mut self) -> Result<Edeltraud<J>, BuildError> where for<'a> JobUnit<'a, J, J>: Job, J: Send + 'static {
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
                        handle: Handle {
                            inner: inner.clone(),
                            threads,
                        },
                        stats: Stats { counters, ..Default::default() },
                    };
                    while let Some(job) = inner.acquire_job(worker_index, &mut worker_thread_pool.stats) {
                        let now = Instant::now();
                        let job_unit = JobUnit {
                            handle: &worker_thread_pool.handle,
                            job,
                        };
                        job_unit.run();
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

impl<J> Edeltraud<J> {
    pub fn handle(&self) -> Handle<J> {
        Handle {
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

impl<J> Drop for Edeltraud<J> {
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

pub struct AsyncJob<G> where G: Computation {
    computation: G,
    result_tx: oneshot::Sender<G::Output>,
}

pub fn job<J, G>(thread_pool: &Handle<J>, job: G) -> Result<(), SpawnError> where J: From<G> {
    thread_pool.spawn(job)
}

pub fn job_async<J, G>(thread_pool: &Handle<J>, computation: G) -> Result<AsyncResult<G::Output>, SpawnError>
where J: From<AsyncJob<G>>,
      G: Computation,
{
    let (result_tx, result_rx) = oneshot::channel();
    let async_job = AsyncJob { computation, result_tx, };
    thread_pool.spawn(async_job)?;

    Ok(AsyncResult { result_rx, })
}

impl<'a, J, G> Job for JobUnit<'a, J, AsyncJob<G>> where G: Computation, /* G::Output: Send, */ {
    fn run(self) {
        let output = self.job.computation.run();
        if let Err(_send_error) = self.job.result_tx.send(output) {
            log::warn!("async result channel dropped before job is finished");
        }
    }
}

struct EdeltraudLocal<J> {
    handle: Handle<J>,
    stats: Stats,
}

impl<J> Drop for EdeltraudLocal<J> {
    fn drop(&mut self) {
        log::info!("EdeltraudLocal::drop on {:?}, stats: {:?}", thread::current(), self.stats);
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
