use futures::{Async, Future, Poll, Sink};
use futures::stream::{Map, Select, Stream};
use futures::sync::mpsc;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Drop;

pub fn new<SubId, Sub, Msg, MsgStream, E>(messages: MsgStream)
    -> (SubscriptionManager<SubId, Sub, Msg>,
        Broadcast<SubId, Sub, EventStream<SubId, Sub, Msg, MsgStream>>)
where
    Msg: Clone,
    SubId: Clone + Eq + Hash,
    Sub: Sink<SinkItem = Msg, SinkError = E>,
    MsgStream: Stream<Item = Msg, Error = ()>,
{
    let (events_tx, events_rx) = mpsc::unbounded::<Event<SubId, Sub, Msg>>();

    let manager = SubscriptionManager(events_tx);

    let event_stream =
        events_rx.select(messages.map(Event::Message as fn(Msg) -> Event<SubId, Sub, Msg>));

    let broadcast = Broadcast {
        events_stream: EventStream(event_stream),
        subscribers: HashMap::<SubId, Sub>::new(),
    };

    (manager, broadcast)
}


pub struct EventStream<SubId, Sub, Msg, MsgStream>(
    Select<mpsc::UnboundedReceiver<Event<SubId, Sub, Msg>>,
            Map<MsgStream, fn(Msg) -> Event<SubId, Sub, Msg>>>
);

impl<SubId, Sub, Msg, MsgStream> Stream for EventStream<SubId, Sub, Msg, MsgStream>
where MsgStream: Stream<Item = Msg, Error = ()>
{
    type Item = Event<SubId, Sub, Msg>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll()
    }
}

pub struct SubscriptionManager<SubId, Sub, Msg>(mpsc::UnboundedSender<Event<SubId, Sub, Msg>>);

impl<SubId, Sub, Msg> SubscriptionManager<SubId, Sub, Msg>
where
    SubId: Clone,
{
    // TODO: Better error
    fn subscribe
        (
        &self,
        id: SubId,
        subscriber: Sub,
    ) -> Result<Subscription<SubId, Sub, Msg>, mpsc::SendError<Event<SubId, Sub, Msg>>> {
        let event = Event::Subscribe {
            id: id.clone(),
            subscriber,
        };
        mpsc::UnboundedSender::send(&self.0, event).map(
            |_| {
                Subscription {
                    id: Some(id),
                    manager: SubscriptionManager(self.0.clone()),
                }
            }
        )
    }

    fn unsubscribe(&self, id: SubId) -> Result<(), mpsc::SendError<Event<SubId, Sub, Msg>>> {
        mpsc::UnboundedSender::send(&self.0, Event::Unsubscribe(id))
    }
}

pub struct Subscription<SubId, Sub, Msg>
where
    SubId: Clone,
{
    id: Option<SubId>,
    manager: SubscriptionManager<SubId, Sub, Msg>,
}

impl<SubId, Sub, Msg> Subscription<SubId, Sub, Msg>
where
    SubId: Clone,
{
    fn unsubscribe(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.manager.unsubscribe(id);
        }
    }
}

impl<SubId, Sub, Msg> Drop for Subscription<SubId, Sub, Msg>
where
    SubId: Clone,
{
    fn drop(&mut self) {
        self.unsubscribe();
    }
}

pub enum Event<SubId, Sub, Msg> {
    Subscribe { id: SubId, subscriber: Sub },
    Unsubscribe(SubId),
    Message(Msg),
}

pub struct Broadcast<SubId, Sub, EventsStream> {
    events_stream: EventsStream,
    subscribers: HashMap<SubId, Sub>,
}

impl<SubId, Sub, EventsStream, Msg, E> Future for Broadcast<SubId, Sub, EventsStream>
where
    Msg: Clone,
    SubId: Eq + Hash,
    Sub: Sink<SinkItem = Msg, SinkError = E>,
    EventsStream: Stream<Item = Event<SubId, Sub, Msg>, Error = ()>
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            if let Some(event) = try_ready!(self.events_stream.poll()) {
                match event {
                    Event::Subscribe { id: sub_id, subscriber: sub } => {
                        println!("Subscribe");
                        self.subscribers.insert(sub_id, sub);
                    },
                    Event::Unsubscribe(sub_id) => {
                        println!("Unsubscribe");
                        self.subscribers.remove(&sub_id);
                    },
                    Event::Message(msg) => {
                        println!("Message");

// If subscriber fails to receive, we don't care
// TODO: Adjust this. Also why does rustfmt put this on the left
                        for (_, sub) in self.subscribers.iter_mut() {
                            println!("Sending Message");
                            let _ = sub.start_send(msg.clone());
                        }
                    }
                }
            } else {
                return Ok(Async::Ready(()));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::{self, Future, Stream};
    use futures::unsync::mpsc;

    #[test]
    fn test_broadcast() {
        // Run on a task context
        futures::lazy(
            || {
                let (msg_tx, msg_rx) = mpsc::unbounded::<&str>();
                let (manager, mut broadcast) = new(msg_rx);

                let mut tick = move || { let _ = broadcast.poll(); };
                let send = move |msg| { let _ = mpsc::UnboundedSender::send(&msg_tx, msg); };

                let (tx1, mut rx1) = mpsc::unbounded::<&str>();
                let (tx2, mut rx2) = mpsc::unbounded::<&str>();

                let sub1 = manager.subscribe(1, tx1);
                tick();

                send("1");
                tick();

                assert_eq!(rx1.poll(), Ok(Async::Ready(Some("1"))));
                assert_eq!(rx2.poll(), Ok(Async::NotReady));

                let sub2 = manager.subscribe(2, tx2);
                tick();

                send("2");
                tick();

                assert_eq!(rx1.poll(), Ok(Async::Ready(Some("2"))));
                assert_eq!(rx2.poll(), Ok(Async::Ready(Some("2"))));

                sub1.unwrap().unsubscribe();
                tick();

                send("3");
                tick();

                assert_eq!(rx1.poll(), Ok(Async::Ready(None)));
                assert_eq!(rx2.poll(), Ok(Async::Ready(Some("3"))));

                sub2.unwrap().unsubscribe();
                tick();

                send("4");
                tick();

                assert_eq!(rx1.poll(), Ok(Async::Ready(None)));
                assert_eq!(rx2.poll(), Ok(Async::Ready(None)));

                Ok::<(), ()>(())
            }
        )
                .wait()
                .unwrap();
    }

    #[test]
    fn unsubscribe_when_out_of_scope() {
        futures::lazy(
            || {
                let (msg_tx, msg_rx) = mpsc::unbounded::<&str>();
                let (manager, mut broadcast) = new(msg_rx);

                let mut tick = move || { let _ = broadcast.poll(); };
                let send = move |msg| { let _ = mpsc::UnboundedSender::send(&msg_tx, msg); };

                let (tx, mut rx) = mpsc::unbounded::<&str>();

                {
                    // TODO: If tx is cloned here, Async::NotReady is returned
                    let sub = manager.subscribe(1, tx);
                    tick();

                    send("1");
                    tick();

                    assert_eq!(rx.poll(), Ok(Async::Ready(Some("1"))));
                }
                tick();

                send("2");
                tick();

                // This returns Async::NotReady if tx is cloned above
                assert_eq!(rx.poll(), Ok(Async::Ready(None)));

                Ok::<(), ()>(())
            }
        )
                .wait()
                .unwrap();
    }
}
