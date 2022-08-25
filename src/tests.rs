use std::{
    thread,
    time::{
        Instant,
        Duration,
    },
    sync::{
        mpsc,
    },
};

use super::{
    Job,
    Builder,
    AsyncJob,
    Edeltraud,
    ThreadPool,
    EdeltraudJobMap,
    job_async,
};

struct SleepJob;

impl Job for SleepJob {
    type Output = ();

    fn run<P>(self, _thread_pool: &P) -> Self::Output where P: ThreadPool<Self> {
        thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn basic() {
    let pool: Edeltraud<AsyncJob<SleepJob>> = Builder::new()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let now = Instant::now();
    runtime.block_on(async move {
        let mut tasks = Vec::new();
        for _ in 0 .. 16 {
            tasks.push(async {
                job_async(&pool, SleepJob).unwrap().await
            });
        }
        let _: Vec<()> = futures::future::try_join_all(tasks).await.unwrap();
    });
    let elapsed = now.elapsed().as_secs_f64();
    assert!(elapsed >= 0.4);
    assert!(elapsed < 0.5);
}

enum SleepJobRec {
    Master,
    Slave { tx: mpsc::Sender<WorkComplete>, },
}

struct WorkComplete;

impl Job for SleepJobRec {
    type Output = ();

    fn run<P>(self, thread_pool: &P) -> Self::Output where P: ThreadPool<Self> {
        match self {
            SleepJobRec::Master => {
                let (tx, rx) = mpsc::channel();
                for _ in 0 .. 4 {
                    thread_pool.spawn(SleepJobRec::Slave { tx: tx.clone(), })
                        .unwrap();
                }
                for _ in 0 .. 4 {
                    let WorkComplete = rx.recv().unwrap();
                }
            },
            SleepJobRec::Slave { tx, } => {
                thread::sleep(Duration::from_millis(400));
                tx.send(WorkComplete).unwrap();
            },
        }
    }
}

#[test]
fn recursive_spawn() {
    let pool: Edeltraud<AsyncJob<SleepJobRec>> = Builder::new()
        .worker_threads(5)
        .build()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let now = Instant::now();
    runtime.block_on(job_async(&pool, SleepJobRec::Master).unwrap())
        .unwrap();
    let elapsed = now.elapsed().as_secs_f64();
    assert!(elapsed >= 0.4);
    assert!(elapsed < 0.5);
}

struct WrappedSleepJob(AsyncJob<SleepJob>);

impl From<AsyncJob<SleepJob>> for WrappedSleepJob {
    fn from(async_job: AsyncJob<SleepJob>) -> WrappedSleepJob {
        WrappedSleepJob(async_job)
    }
}

impl Job for WrappedSleepJob {
    type Output = ();

    fn run<P>(self, thread_pool: &P) -> Self::Output where P: ThreadPool<Self> {
        let WrappedSleepJob(sleep_job) = self;
        sleep_job.run(&EdeltraudJobMap::new(thread_pool))
    }
}

#[test]
fn multilayer_job() {
    let pool: Edeltraud<WrappedSleepJob> = Builder::new()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let now = Instant::now();
    runtime.block_on(async move {
        let mut tasks = Vec::new();
        for _ in 0 .. 16 {
            tasks.push(async {
                job_async(&pool, SleepJob).unwrap().await
            });
        }
        let _: Vec<()> = futures::future::try_join_all(tasks).await.unwrap();
    });
    let elapsed = now.elapsed().as_secs_f64();
    assert!(elapsed >= 0.4);
    assert!(elapsed < 0.5);
}

struct SleepJobValue(isize);

impl Job for SleepJobValue {
    type Output = isize;

    fn run<P>(self, _thread_pool: &P) -> Self::Output where P: ThreadPool<Self> {
        thread::sleep(Duration::from_millis(400));
        self.0
    }
}

#[test]
fn async_job() {
    let pool: Edeltraud<AsyncJob<SleepJobValue>> = Builder::new()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let now = Instant::now();
    let value = runtime.block_on(job_async(&pool, SleepJobValue(144)).unwrap())
        .unwrap();
    let elapsed = now.elapsed().as_secs_f64();
    assert!(elapsed >= 0.4);
    assert!(elapsed < 0.5);
    assert_eq!(value, 144);
}
