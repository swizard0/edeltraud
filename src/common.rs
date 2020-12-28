use futures::{
    channel::{
        oneshot,
    },
};

use super::Job;

pub struct Task<T> where T: Job {
    pub task: T,
    pub reply_tx: oneshot::Sender<T::Output>,
}

pub enum Event<T> where T: Job {
    IncomingTask(Task<T>),
    SlaveOnline { slave_index: usize, },
}
