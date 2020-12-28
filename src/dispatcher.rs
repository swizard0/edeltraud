use crossbeam_channel as channel;

use super::common::{
    Task,
    Event,
};

pub fn run<T, R>(dispatcher_rx: channel::Receiver<Event<T, R>>, slaves: Vec<channel::Sender<Task<T, R>>>) {
    let mut online = Vec::with_capacity(slaves.len());
    let mut tasks = Vec::new();

    loop {
        match dispatcher_rx.recv() {
            Ok(Event::IncomingTask(task)) =>
                match online.pop() {
                    Some(slave_index) => {
                        let slave: &'_ channel::Sender<_> = &slaves[slave_index];
                        if let Err(_send_error) = slave.send(task) {
                            log::error!("slave {} terminated unexpectedly: terminating", slave_index);
                            break;
                        }
                    },
                    None =>
                        tasks.push(task),
                },
            Ok(Event::SlaveOnline { slave_index, }) =>
                match tasks.pop() {
                    Some(task) =>
                        if let Err(_send_error) = slaves[slave_index].send(task) {
                            log::error!("slave {} terminated unexpectedly: terminating", slave_index);
                            break;
                        },
                    None =>
                        online.push(slave_index),
                },
            Err(channel::RecvError) => {
                log::debug!("all dispatcher clients are dropped: terminating");
                break;
            },
        }
    }
}
