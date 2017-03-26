extern crate futures;
extern crate tokio_core;

use futures::{Future, Stream};
use futures::sync::mpsc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::rc::Rc;
use tokio_core::reactor::Core;

#[derive(Debug)]
struct Tweet<'a> {
    id: u32,
    name: &'a str,
}

impl<'a> Tweet<'a> {
    pub fn new(id: u32, name: &'a str) -> Self {
        Tweet {
            id: id,
            name: name,
        }
    }
}

fn main() {
    let mut core = Core::new().unwrap();

    let (tx, rx) = mpsc::unbounded();

    let tweets = vec![Tweet::new(1, "a"),
                      Tweet::new(2, "a"),
                      Tweet::new(3, "b"),
                      Tweet::new(4, "a"),
                      Tweet::new(5, "a"),
                      Tweet::new(6, "b"),
                      Tweet::new(7, "c"),
                      Tweet::new(8, "a")];

    for t in tweets {
        tx.send(t);
    }

    let mut write_streams = HashMap::new();
    let mut streams_map = Rc::new(RefCell::new(HashMap::new()));

    let x = rx.for_each(|tweet| {
        match write_streams.entry(tweet.name) {
            Entry::Vacant(v) => {
                let (tx2, rx2) = mpsc::unbounded();
                let zzz = streams_map.clone();
                zzz.borrow_mut().insert(tweet.name, rx2);
                tx2.send(tweet);
                v.insert(tx2);
            }
            Entry::Occupied(mut o) => {
                let mut stream = o.get_mut();
                stream.send(tweet);
            }
        };

        Ok(())
    });


    let zzz = x.and_then(|_| {
        let z = streams_map.clone();
        let mut zz = z.borrow_mut();
        zz.get_mut("a").unwrap().for_each(|tweet| {
            println!("{:?}", tweet);
            Ok(())
        })
    });

    core.run(zzz).unwrap();

}
