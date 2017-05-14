use futures::{Async, AsyncSink, Poll, Sink, StartSend};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

pub struct Broadcast<Id, S> {
    subscribers: HashMap<Id, S>,
}

impl<Id, Msg, S, E> Broadcast<Id, S>
where
    Id: Eq + Hash,
    Msg: Clone,
    S: Sink<SinkItem = Msg, SinkError = E>,
{
    fn empty() -> Self {
        Broadcast { subscribers: HashMap::new() }
    }

    fn subscribe(&mut self, id: Id, sink: S) -> Option<S> {
        self.subscribers.insert(id, sink)
    }

    fn unsubscribe(&mut self, id: &Id) -> Option<S> {
        self.subscribers.remove(id)
    }
}

impl<Id, S, Msg, E> Sink for Broadcast<Id, S>
where
    Id: Eq + Hash,
    Msg: Clone,
    S: Sink<SinkItem = Msg, SinkError = E>,
{
    type SinkItem = Msg;
    type SinkError = ();

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        for (_, sub) in self.subscribers.iter_mut() {
            // If subscriber fails to receive, we don't care
            let _ = sub.start_send(item.clone());
        }

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}
