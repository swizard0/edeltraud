use crossbeam_channel as channel;

use super::{
    common::{
        Task,
        Event,
    },
    Job,
};

pub fn run<T, R>(
    dispatcher_tx: channel::Sender<Event<T, R>>,
    slave_rx: channel::Receiver<Task<T, R>>,
    slave_index: usize,
)
    where T: Job<Output = R>,
{
    loop {
        if let Err(_send_error) = dispatcher_tx.send(Event::SlaveOnline { slave_index, }) {
            log::debug!("dispatcher is gone (dispatcher_tx), terminating");
            break;
        }

        match slave_rx.recv() {
            Ok(Task { task, reply_tx, }) => {
                let output = task.run();
                if let Err(_send_error) = reply_tx.send(output) {
                    log::debug!("job output receiver is dropped, ignoring");
                }
            },
            Err(channel::RecvError) => {
                log::debug!("dispatcher is gone (slave_rx), terminating");
                break;
            },
        }
    }
}
