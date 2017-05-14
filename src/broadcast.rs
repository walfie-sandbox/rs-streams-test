use futures::{Async, AsyncSink, Poll, Sink, StartSend};
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Drop;
use std::sync::mpsc;

enum Event<Id, S> {
    Subscribe(Id, S),
    Unsubscribe(Id),
}

pub struct Subscription<'a, Id: 'a, S: 'a> {
    id: Option<Id>,
    broadcaster: &'a Broadcast<Id, S>,
}

impl<'a, Id, S> Drop for Subscription<'a, Id, S> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.broadcaster.unsubscribe(id);
        }
    }
}

pub struct Broadcast<Id, S> {
    subscribers: HashMap<Id, S>,
    events_sender: mpsc::Sender<Event<Id, S>>,
    events_receiver: mpsc::Receiver<Event<Id, S>>,
}

impl<Id, S> Broadcast<Id, S> {
    pub fn unsubscribe(&self, id: Id) {
        let _ = self.events_sender.send(Event::Unsubscribe(id));
    }
}

impl<Id, Msg, S, E> Broadcast<Id, S>
where
    Id: Clone + Eq + Hash,
    Msg: Clone,
    S: Sink<SinkItem = Msg, SinkError = E>,
{
    pub fn empty() -> Self {
        let (sender, receiver) = mpsc::channel();

        Broadcast {
            subscribers: HashMap::new(),
            events_sender: sender,
            events_receiver: receiver,
        }
    }

    pub fn subscribe(&self, id: Id, sink: S) -> Subscription<Id, S> {
        let _ = self.events_sender.send(Event::Subscribe(id.clone(), sink));

        Subscription {
            id: Some(id),
            broadcaster: self,
        }
    }

    fn sync_subscribers(&mut self) {
        for event in self.events_receiver.try_iter() {
            match event {
                Event::Subscribe(id, sink) => self.subscribers.insert(id, sink),
                Event::Unsubscribe(id) => self.subscribers.remove(&id),
            };
        }
    }
}

impl<Id, S, Msg, E> Sink for Broadcast<Id, S>
where
    Id: Clone + Eq + Hash,
    Msg: Clone,
    S: Sink<SinkItem = Msg, SinkError = E>,
{
    type SinkItem = Msg;
    type SinkError = ();

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.sync_subscribers();

        for (_, sub) in self.subscribers.iter_mut() {
            // If subscriber fails to receive, we don't care
            let _ = sub.start_send(item.clone());
        }

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.sync_subscribers();

        Ok(Async::Ready(()))
    }
}
