use std::{
    thread,
    time::{
        Instant,
        Duration,
    },
    sync::{
        mpsc,
        atomic,
        Arc,
    },
};

use super::{
    Job,
    Builder,
    AsyncJob,
    Edeltraud,
    ThreadPool,
    Computation,
    ThreadPoolMap,
    job,
    job_async,
};

struct SleepJob;

impl Computation for SleepJob {
    type Output = ();

    fn run(self) {
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
    assert!(elapsed >= 0.4, "elapsed expected to be >= 0.4, but it is {elapsed:?}");
    assert!(elapsed < 0.5, "elapsed expected to be < 0.5, but it is {elapsed:?}");
}

enum SleepJobRec {
    Master { sync_tx: mpsc::Sender<WorkComplete>, },
    Slave { tx: mpsc::Sender<WorkComplete>, },
}

struct WorkComplete;

impl Job for SleepJobRec {
    fn run<P>(self, thread_pool: &P) where P: ThreadPool<Self> {
        match self {
            SleepJobRec::Master { sync_tx, } => {
                let (tx, rx) = mpsc::channel();
                for _ in 0 .. 4 {
                    thread_pool.spawn(SleepJobRec::Slave { tx: tx.clone(), })
                        .unwrap();
                }
                for _ in 0 .. 4 {
                    let WorkComplete = rx.recv().unwrap();
                }
                sync_tx.send(WorkComplete).ok();
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
    let pool: Edeltraud<SleepJobRec> = Builder::new()
        .worker_threads(5)
        .build()
        .unwrap();
    let now = Instant::now();

    let (sync_tx, sync_rx) = mpsc::channel();
    job(&pool, SleepJobRec::Master { sync_tx, }).unwrap();
    let WorkComplete = sync_rx.recv().unwrap();

    let elapsed = now.elapsed().as_secs_f64();
    assert!(elapsed >= 0.4, "elapsed expected to be >= 0.4, but it is {elapsed:?}");
    assert!(elapsed < 0.5, "elapsed expected to be < 0.5, but it is {elapsed:?}");
}

struct WrappedSleepJob(AsyncJob<SleepJob>);

impl From<AsyncJob<SleepJob>> for WrappedSleepJob {
    fn from(async_job: AsyncJob<SleepJob>) -> WrappedSleepJob {
        WrappedSleepJob(async_job)
    }
}

impl Job for WrappedSleepJob {
    fn run<P>(self, thread_pool: &P) where P: ThreadPool<Self> {
        let WrappedSleepJob(sleep_job) = self;
        sleep_job.run(&ThreadPoolMap::new(thread_pool))
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
    assert!(elapsed >= 0.4, "elapsed expected to be >= 0.4, but it is {elapsed:?}");
    assert!(elapsed < 0.5, "elapsed expected to be < 0.5, but it is {elapsed:?}");
}

struct SleepJobValue(isize);

impl Computation for SleepJobValue {
    type Output = isize;

    fn run(self) -> Self::Output {
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
    assert!(elapsed >= 0.4, "elapsed expected to be >= 0.4, but it is {elapsed:?}");
    assert!(elapsed < 0.5, "elapsed expected to be < 0.5, but it is {elapsed:?}");
    assert_eq!(value, 144);
}

#[test]
fn small_stress_job() {
    const JOBS_COUNT: usize = 256;
    const SUBJOBS_COUNT: usize = 256;

    let shared_counter = Arc::new(atomic::AtomicUsize::new(0));

    struct StressJob {
        shared_counter: Arc<atomic::AtomicUsize>,
        allow_rec: bool,
    }

    impl Job for StressJob {
        fn run<P>(self, thread_pool: &P) where P: ThreadPool<Self> {
            if self.allow_rec {
                for _ in 0 .. SUBJOBS_COUNT {
                    thread_pool
                        .spawn(StressJob {
                            shared_counter: self.shared_counter.clone(),
                            allow_rec: false,
                        })
                        .unwrap();
                }
            } else {
                self.shared_counter.fetch_add(1, atomic::Ordering::Relaxed);
            }
        }
    }

    let thread_pool: Edeltraud<StressJob> = Builder::new()
        .worker_threads(4)
        .build()
        .unwrap();

    for _ in 0 .. JOBS_COUNT {
        thread_pool.spawn(StressJob { shared_counter: shared_counter.clone(), allow_rec: true, })
            .unwrap();
    }

    while shared_counter.load(atomic::Ordering::Relaxed) < JOBS_COUNT * SUBJOBS_COUNT {
        thread::sleep(Duration::from_millis(50));
    }
}
