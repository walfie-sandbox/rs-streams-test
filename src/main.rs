extern crate futures;
extern crate tokio_core;
extern crate multiqueue;

use futures::{Future, Stream};
use futures::sink::Sink;
use futures::sync::mpsc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::rc::Rc;
use std::sync::Arc;
use tokio_core::reactor::Core;

#[derive(PartialEq, Hash)]
struct Consumer;

impl Sink for Consumer {
    type SinkItem = String;
    type SinkError = ();

    fn start_send(
        &mut self,
        item: Self::SinkItem
    ) -> futures::StartSend<Self::SinkItem, Self::SinkError> {
        println!("{}", item);
        Ok(futures::AsyncSink::Ready)
    }
    fn poll_complete(&mut self) -> futures::Poll<(), Self::SinkError> {
        Ok(futures::Async::Ready(()))
    }
}

fn main() {
    let (tx, rx) = multiqueue::broadcast_fut_queue(25);

    let consumer = Consumer;

    let f = rx.add_stream().forward(consumer);

    tx.try_send("Hello!".to_string()).unwrap();
    let mut core = Core::new().unwrap();
    core.run(f).unwrap();
}
