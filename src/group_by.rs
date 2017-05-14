use futures::{Async, Poll};
use futures::stream::Stream;
use futures::unsync::mpsc;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;

#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
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

#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct GroupBy<K, V, S, F>
where
    K: Eq + Hash,
{
    writers: HashMap<K, mpsc::UnboundedSender<V>>,
    stream: S,
    key_selector: F,
}

pub fn new<K, V, S, F>(stream: S, key_selector: F) -> GroupBy<K, V, S, F>
where
    K: Clone + Eq + Hash,
    S: Stream<Item = V, Error = ()>,
    F: Fn(&V) -> K,
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
    S: Stream<Item = V, Error = ()>,
    F: Fn(&V) -> K,
{
    type Item = GroupedStream<K, V>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            if let Some(value) = try_ready!(self.stream.poll()) {
                let key = (self.key_selector)(&value);

                match self.writers.entry(key.clone()) {
                    Entry::Occupied(entry) => {
                        let _ = mpsc::UnboundedSender::send(&entry.get(), value); // Ignore error
                    }
                    Entry::Vacant(entry) => {
                        let (tx, rx) = mpsc::unbounded();
                        let grouped = GroupedStream {
                            key: key,
                            underlying: rx,
                        };

                        let _ = mpsc::UnboundedSender::send(&tx, value); // Ignore error
                        entry.insert(tx);
                        return Ok(Async::Ready(Some(grouped)));
                    }
                }
            }
        }
    }
}
