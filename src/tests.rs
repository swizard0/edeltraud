use std::{
    thread,
    time::{
        Instant,
        Duration,
    },
};

use super::{
    Job,
    Builder,
    Edeltraud,
};

#[test]
fn basic() {
    struct SleepJob;

    impl Job for SleepJob {
        type Output = ();

        fn run(self) -> Self::Output {
            thread::sleep(Duration::from_millis(100));
        }
    }

    let pool: Edeltraud<SleepJob, ()> = Builder::new()
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
            let local_pool = pool.clone();
            tasks.push(async move {
                local_pool.spawn(SleepJob).await
            });
        }
        let _: Vec<()> = futures::future::try_join_all(tasks).await.unwrap();
    });
    let elapsed = now.elapsed().as_secs_f64();
    assert!(elapsed >= 0.4);
    assert!(elapsed < 0.5);
}
