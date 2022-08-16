use futures::{
    channel::{
        oneshot,
    },
};

use crate::{
    Job,
};

pub struct Task<T> where T: Job {
    pub task: T,
    pub task_reply: TaskReply<T::Output>,
}

pub enum TaskReply<R> {
    None,
    Oneshot {
        reply_tx: oneshot::Sender<R>,
    },
}
