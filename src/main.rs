extern crate futures;
extern crate tokio_core;

use futures::{Future, Stream};
use futures::sink::Sink;
use futures::sync::mpsc;
use std::borrow::ToOwned;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use tokio_core::reactor::{Core, Handle};

struct GroupedStream<K, V>
    where V: 'static
{
    pub key: K,
    stream: mpsc::UnboundedReceiver<V>,
}

impl<K, V> Deref for GroupedStream<K, V> {
    type Target = mpsc::UnboundedReceiver<V>;
    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

struct GroupBy<K, V>
    where K: 'static,
          V: 'static
{
    stream: mpsc::UnboundedReceiver<GroupedStream<K, V>>,
    writers: Rc<RefCell<HashMap<K, mpsc::UnboundedSender<V>>>>,
}

impl<K, V> Deref for GroupBy<K, V> {
    type Target = mpsc::UnboundedReceiver<GroupedStream<K, V>>;
    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl<K, V> GroupBy<K, V> {
    pub fn new<F, S, E>(handle: &Handle, input_stream: S, key_selector: F) -> Self
        where S: Stream<Item = V, Error = E> + 'static,
              F: Fn(&V) -> K + 'static,
              K: Clone + Hash + Eq
    {
        let writers0: Rc<RefCell<HashMap<K, mpsc::UnboundedSender<V>>>> =
            Rc::new(RefCell::new(HashMap::new()));

        let (tx, rx) = mpsc::unbounded::<GroupedStream<K, V>>();

        let writers = writers0.clone();

        let handle0 = handle.clone();
        let process_input = input_stream
            .map_err(|_| ())
            .and_then(move |value| {
                let key = key_selector(&value);

                match writers.borrow_mut().entry(key) {
                    Entry::Vacant(v) => {
                        let (grouped_tx, grouped_rx) = mpsc::unbounded();
                        let grouped_stream = GroupedStream {
                            key: v.key().clone(),
                            stream: grouped_rx,
                        };
                        let process_new_entry = tx.clone().send(grouped_stream);
                        v.insert(grouped_tx.clone());
                        handle.spawn(grouped_tx.send(value).map(|_| ()).map_err(|_| ()));
                    }
                    Entry::Occupied(mut o) => {
                        handle.spawn(o.get_mut().send(value).map(|_| ()).map_err(|_| ()));
                    }
                };

                Ok(())
            })
            .for_each(|_| Ok(()));

        handle.spawn(process_input);

        GroupBy {
            stream: rx,
            writers: writers0,
        }
    }
}

#[derive(Debug)]
struct Tweet {
    user: String,
    text: String,
}

impl Tweet {
    fn new<U, T>(user: U, text: T) -> Self
        where U: Into<String>,
              T: Into<String>
    {
        Tweet {
            user: user.into(),
            text: text.into(),
        }
    }
}

fn main() {
    let mut core = Core::new().expect("failed to instantiate Core");
    let handle = core.handle();

    let (tx, rx) = mpsc::unbounded::<Tweet>();

    let grouped = GroupBy::new(&handle.clone(), rx, |ref tweet| tweet.user.clone());

    let group_handle = handle.clone();
    let task = grouped
        .stream
        .for_each(move |group| {
            let key = group.key;
            println!("new group: {}", key);

            let task = group
                .stream
                .for_each(move |item| {
                              println!("{}: {:?}", key, item);
                              Ok(())
                          });

            group_handle.spawn(task);
            Ok(())
        });

    handle.spawn(task);

    let tweets =
        vec![Tweet::new("walf", "hello"), Tweet::new("mil", "nyao"), Tweet::new("walf", "world")];

    for tweet in tweets {
        tx.clone().send(tweet);
    }


    let empty = futures::empty::<(), ()>();
    core.run(empty).unwrap();
}
