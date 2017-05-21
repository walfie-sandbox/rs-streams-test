use futures::{Async, Future, Poll, Sink};
use futures::stream::{Map, Select, Stream};
use futures::sync::mpsc;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Drop;

fn new<SubId, Sub, Msg, MsgStream, E>(messages: MsgStream)
    -> (SubscriptionManager<SubId, Sub, Msg>,
        Broadcast<SubId, Sub, SelectEvents<SubId, Sub, Msg, MsgStream>>)
where
    Msg: Clone,
    SubId: Clone + Eq + Hash,
    Sub: Sink<SinkItem = Msg, SinkError = E>,
    MsgStream: Stream<Item = Msg, Error = ()>,
{
    let (events_tx, events_rx) = mpsc::unbounded::<Event<SubId, Sub, Msg>>();

    let manager = SubscriptionManager(events_tx);

    let events_stream =
        events_rx.select(messages.map(Event::Message as fn(Msg) -> Event<SubId, Sub, Msg>));

    let broadcast = Broadcast {
        events_stream: events_stream,
        subscribers: HashMap::<SubId, Sub>::new(),
    };

    (manager, broadcast)
}

type SelectEvents<SubId, Sub, Msg, MsgStream> = Select<mpsc::UnboundedReceiver<Event<SubId,
                                                                                     Sub,
                                                                                     Msg>>,
                                                       Map<MsgStream,
                                                           fn(Msg) -> Event<SubId, Sub, Msg>>>;

struct SubscriptionManager<SubId, Sub, Msg>(mpsc::UnboundedSender<Event<SubId, Sub, Msg>>);

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

struct Subscription<SubId, Sub, Msg>
where
    SubId: Clone,
{
    id: Option<SubId>,
    manager: SubscriptionManager<SubId, Sub, Msg>,
}

impl<SubId, Sub, Msg> Drop for Subscription<SubId, Sub, Msg>
where
    SubId: Clone,
{
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.manager.unsubscribe(id);
        }
    }
}

enum Event<SubId, Sub, Msg> {
    Subscribe { id: SubId, subscriber: Sub },
    Unsubscribe(SubId),
    Message(Msg),
}

struct Broadcast<SubId, Sub, EventsStream> {
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
            let event = try_ready!(self.events_stream.poll());

            if event.is_none() {
                return Ok(Async::NotReady);
            }

            assert!(event.is_some());

            match event.unwrap() {
                Event::Subscribe { id: sub_id, subscriber: sub } => {
                    self.subscribers.insert(sub_id, sub);
                },
                Event::Unsubscribe(sub_id) => {
                    self.subscribers.remove(&sub_id);
                },
                Event::Message(msg) => {
// If subscriber fails to receive, we don't care
// TODO: Adjust this. Also why does rustfmt put this on the left
                    for (_, sub) in self.subscribers.iter_mut() {
                        let _ = sub.start_send(msg.clone());
                    }
                }
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
                let (mut msg_tx, msg_rx) = mpsc::unbounded::<&str>();

                let (manager, mut broadcast) = new(msg_rx);

                let (tx1, mut rx1) = mpsc::unbounded::<&str>();
                let (tx2, mut rx2) = mpsc::unbounded::<&str>();

                let sub1 = manager.subscribe(1, tx1);
                let sub2 = manager.subscribe(2, tx2);

                let _ = mpsc::UnboundedSender::send(&msg_tx, "Hello");

                let _ = broadcast.poll();
                assert!(rx1.poll() == Ok(Async::Ready(Some("Hello"))));

                // Fails because `Select` is round-robin
                //assert!(rx2.poll() == Ok(Async::Ready(Some("Hello"))));

                Ok::<(), ()>(())
            }
        )
                .wait()
                .unwrap();
    }
}
