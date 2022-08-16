use crate::{
    common::{
        Task,
        TaskReply,
    },
    inner::{
        Inner,
    },
    Job,
};

pub fn run<T>(inner: &Inner<T>, slave_index: usize) where T: Job {
    loop {
        let Task { task, task_reply, } = inner.acquire_task();
        let output = task.run();
        match task_reply {
            TaskReply::None =>
                (),
            TaskReply::Oneshot { reply_tx, } =>
                if let Err(_send_error) = reply_tx.send(output) {
                    log::debug!("job output receiver is dropped in slave {slave_index}, ignoring");
                },
        }
    }
}
