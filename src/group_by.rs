use futures::{Async, Poll};
use futures::stream::Stream;
use futures::sync::mpsc;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;

pub struct GroupedStream<K, V> {
    pub key: K,
    underlying: mpsc::UnboundedReceiver<V>,
}

impl<K, V> Stream for GroupedStream<K, V> {
    type Item = V;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.underlying.poll()
    }
}

pub struct GroupBy<K, V, S, F> {
    writers: HashMap<K, mpsc::UnboundedSender<V>>,
    stream: S,
    key_selector: F,
}

pub fn new<K, V, S, F>(stream: S, key_selector: F) -> GroupBy<K, V, S, F>
where
    K: Clone + Eq + Hash,
    V: Clone,
    S: Stream<Item = V, Error = ()>,
    F: Fn(V) -> K,
{
    GroupBy {
        writers: HashMap::new(),
        stream: stream,
        key_selector: key_selector,
    }
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
        if let Some(value) = try_ready!(self.stream.poll()) {
            let key = (self.key_selector)(value.clone());

            match self.writers.entry(key.clone()) {
                Entry::Occupied(mut entry) => {
                    let _ = entry.get_mut().send(value); // Ignore error
                    return Ok(Async::Ready(None));
                }
                Entry::Vacant(entry) => {
                    let (tx, rx) = mpsc::unbounded();
                    let grouped = GroupedStream {
                        key: key,
                        underlying: rx,
                    };

                    let _ = tx.send(value); // Ignore error
                    entry.insert(tx);
                    return Ok(Async::Ready(Some(grouped)));
                }
            }
        } else {
            return Ok(Async::Ready(None));
        }
    }
}
