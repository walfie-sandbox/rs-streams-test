extern crate futures;
extern crate tokio_core;
extern crate chashmap;

use chashmap::CHashMap;
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
struct Consumer {
    pub addr: u32,
}

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
    let subscribers: Arc<CHashMap<u32, Consumer>> = Arc::new(CHashMap::new());

    let (tx, rx) = mpsc::unbounded();

    let publish = rx.for_each(|msg: String| {
        let subs = subscribers.clone();
        for (_, subscriber) in subs.into_iter() {
            subscriber.send(msg.clone());
        }

        Ok(())
    });

    let consumer = Consumer { addr: 1 };
    subscribers.clone().insert(consumer.addr, consumer);

    tx.send("Hello!".to_string());
}
