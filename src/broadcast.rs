use futures::{Async, AsyncSink, Poll, Sink, StartSend};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Drop;
use std::sync::mpsc;

#[derive(Debug)]
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
        /*
        if let Some(id) = self.id.take() {
            self.broadcaster.unsubscribe(id);
        }
        */
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

impl<Id, Msg, S> Broadcast<Id, S>
where
    Id: Clone + Eq + Hash,
    Msg: Clone,
    S: Sink<SinkItem = Msg>,
{
    pub fn new() -> Self {
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
        println!("syncing subscriber mesages");

        for event in self.events_receiver.try_iter() {
            println!("received subscriber message");
            match event {
                Event::Subscribe(id, sink) => {
                    self.subscribers.insert(id, sink);
                    println!("subscribe");
                }
                Event::Unsubscribe(id) => {
                    self.subscribers.remove(&id);
                    println!("unsubscribe");
                }
            };
        }
    }
}

impl<Id, S, Msg> Sink for Broadcast<Id, S>
where
    Id: Clone + Eq + Hash,
    Msg: Clone + Debug,
    S: Sink<SinkItem = Msg> + Debug,
{
    type SinkItem = Msg;
    type SinkError = ();

    fn start_send(&mut self, item: Msg) -> StartSend<Msg, ()> {
        self.sync_subscribers();

        println!("yo");

        for (_, sub) in self.subscribers.iter_mut() {
            println!("{:?} {:?}", sub, item);

            // If subscriber fails to receive, we don't care
            let _ = sub.start_send(item.clone());
        }

        println!("done");

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), ()> {
        self.sync_subscribers();

        Ok(Async::Ready(()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::Stream;
    use futures::unsync::mpsc;

    #[test]
    fn test_broadcast() {
        let (tx1, mut rx1) = mpsc::unbounded::<&str>();
        let (tx2, mut rx2) = mpsc::unbounded::<&str>();
        let (tx3, mut rx3) = mpsc::unbounded::<&str>();

        let mut broadcast = Broadcast::new();
        let _ = broadcast.subscribe(1, tx1);

        let _ = broadcast.start_send("Hello");

        assert!(rx1.poll() == Ok(Async::Ready(Some("Hello"))));
    }
}
