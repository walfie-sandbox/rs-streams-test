extern crate futures;
extern crate tokio_core;

use futures::{Future, Stream};
use futures::sink::Sink;
use futures::sync::mpsc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;
use tokio_core::reactor::Core;

#[derive(Clone, Debug)]
struct Subscriber<Id, Msg> {
    pub id: Id,
    tx: mpsc::UnboundedSender<Msg>,
}

#[derive(Clone, Debug)]
struct Subscribers<Id, Msg>(Rc<RefCell<HashMap<Id, Subscriber<Id, Msg>>>>)
    where Id: Hash + Eq + Debug;

impl<Id, Msg> Subscribers<Id, Msg>
    where Id: Clone + Debug + Eq + Hash,
          Msg: Clone + Debug + 'static
{
    fn new() -> Self {
        Subscribers(Rc::new(RefCell::new(HashMap::new())))
    }

    fn insert(&self, subscriber: Subscriber<Id, Msg>) {
        self.0.borrow_mut().insert(subscriber.id.clone(), subscriber);
    }

    fn remove(&self, id: &Id) -> Option<Subscriber<Id, Msg>> {
        self.0.borrow_mut().remove(id)
    }

    fn broadcast<E: 'static>(&self, msg: Msg) -> Box<Future<Item = (), Error = E>> {
        let subscribers = self.0.borrow();

        let sends = subscribers.values().map(|subscriber| subscriber.tx.clone().send(msg.clone()));

        let sends_stream = ::futures::stream::futures_unordered(sends).then(|_| Ok(()));

        Box::new(sends_stream.for_each(|_| Ok(())))
    }
}

fn main() {
    let mut core = Core::new().expect("failed to instantiate Core");
    let handle = core.handle();

    let subscribers = Subscribers::new();

    for id in 0..10 {
        let (tx, rx) = mpsc::unbounded();

        let subscriber = Subscriber { id: id, tx: tx };

        let job = rx.for_each(move |msg| {
            println!("{}: {}", id, msg);
            Ok(())
        });

        subscribers.insert(subscriber);
        handle.spawn(job);
    }

    let broadcast = subscribers.broadcast::<()>("Hello world!".to_string());

    let empty = futures::empty::<(), ()>();

    core.run(broadcast.then(|_| empty)).expect("failed to run");
}
