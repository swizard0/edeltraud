use futures::{
    channel::{
        oneshot,
    },
};

pub struct Task<T, R> {
    pub task: T,
    pub reply_tx: oneshot::Sender<R>,
}

pub enum Event<T, R> {
    IncomingTask(Task<T, R>),
    SlaveOnline { slave_index: usize, },
}
