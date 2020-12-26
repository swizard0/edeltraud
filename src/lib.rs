use std::{
    io,
    thread,
    sync::{
        mpsc,
    },
};

mod slave;
mod dispatcher;

#[derive(Clone)]
pub struct Edeltraud<T> {
    dispatcher_tx: mpsc::Sender<dispatcher::Event<T>>,
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

    pub fn build<T>(&mut self) -> Result<Edeltraud<T>, Error> where T: Send + 'static {
        let worker_threads = self.worker_threads
            .unwrap_or_else(|| num_cpus::get());

        let (dispatcher_tx, dispatcher_rx) = mpsc::channel();
        let mut slaves = Vec::with_capacity(worker_threads);

        for slave_index in 0 .. worker_threads {
            let (slave_tx, slave_rx) = mpsc::channel();
            slaves.push(slave_tx);
            let dispatcher_tx = dispatcher_tx.clone();
            thread::Builder::new()
                .name(format!("edeltraud worker {}", slave_index))
                .spawn(move || slave::run(dispatcher_tx, slave_rx, slave_index))
                .map_err(Error::WorkerSpawn)?;
        }

        thread::Builder::new()
            .name("edeltraud dispatcher".to_string())
            .spawn(move || dispatcher::run(dispatcher_rx, slaves))
            .map_err(Error::DispatcherSpawn)?;

        Ok(Edeltraud { dispatcher_tx, })
    }
}

#[derive(Debug)]
pub enum Error {
    DispatcherSpawn(io::Error),
    WorkerSpawn(io::Error),
}

impl<T> Edeltraud<T> {

}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
