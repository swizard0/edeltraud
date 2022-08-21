use crate::{
    common::{
        Task,
        TaskReply,
    },
    Job,
    Edeltraud,
};

pub fn run<T>(thread_pool: &Edeltraud<T>, slave_index: usize) where T: Job {
    loop {
        let Task { task, task_reply, } = thread_pool
            .inner
            .acquire_task();
        let output = task.run(thread_pool);
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
