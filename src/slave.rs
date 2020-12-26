use std::{
    sync::{
        mpsc,
    },
};

use super::dispatcher::Event;

pub fn run<T>(dispatcher_tx: mpsc::Sender<Event<T>>, slave: mpsc::Receiver<T>, slave_index: usize) {


}
