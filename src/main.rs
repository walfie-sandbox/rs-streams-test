extern crate futures;
extern crate tokio_core;

use futures::{Future, Stream};
use futures::sink::Sink;
use futures::sync::mpsc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;
use tokio_core::reactor::{Core, Handle};

#[derive(Clone, Debug)]
struct Subscriber<Id, Msg> {
    pub id: Id,
    sink: mpsc::UnboundedSender<Rc<Msg>>,
}

impl<Id, Msg> Subscriber<Id, Msg>
    where Msg: 'static
{
    pub fn new<F>(handle: &Handle, id: Id, on_msg: F) -> Self
        where F: Fn(&Msg) -> () + 'static
    {
        use std::ops::Deref;

        let (tx, rx) = mpsc::unbounded();

        // TODO: What if this subscriber is dropped? does this still run?
        handle.spawn(rx.for_each(move |msg: Rc<Msg>| {
            on_msg(msg.deref());
            Ok(())
        }));

        Subscriber { id: id, sink: tx }
    }
}

type SharedHashMap<K, V> = Rc<RefCell<HashMap<K, V>>>;

#[derive(Clone, Debug)]
struct Subscribers<Id, Msg>(SharedHashMap<Id, Subscriber<Id, Msg>>) where Id: Hash + Eq;

impl<Id, Msg> Subscribers<Id, Msg>
    where Id: Clone + Eq + Hash,
          Msg: 'static
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

    fn broadcast(&self, msg: Msg) -> Box<Future<Item = (), Error = ()>> {
        let subscribers = self.0.borrow();

        let rc_msg = Rc::new(msg);
        let sends = subscribers.values()
            .map(|subscriber| subscriber.sink.clone().send(rc_msg.clone()));

        let sends_stream = ::futures::stream::futures_unordered(sends);

        Box::new(sends_stream.for_each(|_| Ok(())).map_err(|_| ()))
    }
}

struct StreamPartitioner<K, V, SId>
    where SId: Clone + Eq + Hash
{
    partitions: SharedHashMap<K, Subscribers<SId, V>>,
}

impl<K, V, SId> StreamPartitioner<K, V, SId>
    where K: Eq + Hash + 'static,
          V: 'static,
          SId: Clone + Eq + Hash + 'static
{
    pub fn new<S, F, E>(handle: &Handle, input_stream: S, group_by: F) -> Self
        where S: Stream<Item = V, Error = E> + 'static,
              F: Fn(&V) -> K + 'static,
              E: 'static
    {
        let partitions = Rc::new(RefCell::new(HashMap::new()));

        let partitioner = StreamPartitioner { partitions: partitions.clone() };

        let partitioning = input_stream.map_err(|_| ())
            .and_then(move |msg| {
                let mut map = partitions.borrow_mut();

                let key = group_by(&msg);

                match map.entry(key) {
                    Entry::Vacant(v) => {
                        let subs: Subscribers<SId, V> = Subscribers::new();
                        v.insert(subs);
                        Box::new(::futures::future::ok(()))
                    }
                    Entry::Occupied(o) => o.get().broadcast(msg),
                }
            })
            .for_each(|_| Ok(()));

        handle.spawn(partitioning);
        partitioner
    }

    pub fn subscribe(&self, subscriber: Subscriber<SId, V>, key: K) {
        match self.partitions.borrow_mut().entry(key) {
            Entry::Vacant(v) => {
                let subs: Subscribers<SId, V> = Subscribers::new();
                subs.insert(subscriber);
                v.insert(subs);
            }
            Entry::Occupied(o) => {
                o.get().insert(subscriber);
            }
        }
    }

    pub fn unsubscribe(&self, subscriber_id: &SId, key: &K) {
        if let Some(subscribers) = self.partitions.borrow_mut().get(key) {
            subscribers.remove(subscriber_id);
        }
    }
}

#[derive(Clone, Debug)]
struct Tweet {
    user: String,
    text: String,
}

impl Tweet {
    pub fn new<S: Into<String>>(user: S, text: S) -> Self {
        Tweet {
            user: user.into(),
            text: text.into(),
        }
    }
}

fn main() {
    let mut core = Core::new().expect("failed to instantiate Core");
    let handle = core.handle();

    /*
    let subscribers = Subscribers::new();

    for id in 0..10 {
        let (tx, rx) = mpsc::unbounded();

        let subscriber = Subscriber { id: id, sink: tx };

        let job = rx.for_each(move |msg| {
            println!("{}: {}", id, msg);
            Ok(())
        });

        subscribers.insert(subscriber);
        handle.spawn(job);
    }

    subscribers.remove(&5);

    let broadcast = subscribers.broadcast(&"Hello world!".to_string());

    let empty = futures::empty::<(), ()>();

    core.run(broadcast.then(|_| empty)).expect("failed to run");

    println!("yo");
    */
    let (tx, rx) = mpsc::unbounded();

    let partitioner = StreamPartitioner::new(&handle, rx, |t: &Tweet| t.user.clone());

    let subs = (0..10)
        .into_iter()
        .map(|id| {
            Subscriber::new(&handle, id, move |t| {
                println!("{}: {:?}", id, t);
            })
        })
        .collect::<Vec<_>>();

    for i in 0..3 {
        partitioner.subscribe(subs[i].clone(), "a".to_string());
    }

    for i in 2..7 {
        partitioner.subscribe(subs[i].clone(), "b".to_string());
    }

    for i in 6..10 {
        partitioner.subscribe(subs[i].clone(), "c".to_string());
    }

    let tweets = [Tweet::new("a", "hello1"),
                  Tweet::new("b", "hello2"),
                  Tweet::new("c", "hello3"),
                  Tweet::new("a", "hello4"),
                  Tweet::new("b", "hello5"),
                  Tweet::new("c", "hello6"),
                  Tweet::new("a", "hello7"),
                  Tweet::new("b", "hello8"),
                  Tweet::new("c", "hello9")];

    let sends = for tweet in tweets.into_iter().cloned() {
        handle.spawn(tx.clone().send(tweet).map(|_| ()).map_err(|_| ()));
    };

    let empty = futures::empty::<(), ()>();
    core.run(empty).unwrap();
}
