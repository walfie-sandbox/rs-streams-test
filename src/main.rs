#[macro_use]
extern crate futures;

use futures::{Async, Poll};
use futures::stream::Stream;
use futures::sync::mpsc;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;

struct GroupedStream<K, V> {
    key: K,
    underlying: mpsc::UnboundedReceiver<V>,
}

impl<K, V> Stream for GroupedStream<K, V> {
    type Item = V;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.underlying.poll()
    }
}

struct GroupBy<K, V, S, F> {
    writers: HashMap<K, mpsc::UnboundedSender<V>>,
    stream: S,
    key_selector: F,
}

impl<K, V, S, F> Stream for GroupBy<K, V, S, F>
where
    K: Clone + Eq + Hash,
    V: Clone,
    S: Stream<Item = V, Error = ()>,
    F: Fn(V) -> K,
{
    type Item = GroupedStream<K, V>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            if let Some(value) = try_ready!(self.stream.poll()) {
                let key = (self.key_selector)(value.clone());

                match self.writers.entry(key.clone()) {
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().send(value);
                        return Ok(Async::Ready(None));
                    }
                    Entry::Vacant(entry) => {
                        let (tx, rx) = mpsc::unbounded();
                        let grouped = GroupedStream {
                            key: key,
                            underlying: rx,
                        };

                        tx.send(value);
                        entry.insert(tx);
                        return Ok(Async::Ready(Some(grouped)));
                    }
                }
            } else {
                return Ok(Async::Ready(None));
            }
        }

    }
}

fn main() {
    println!("Hello, world!");
}
