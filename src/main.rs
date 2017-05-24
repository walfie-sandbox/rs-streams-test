#[macro_use]
extern crate futures;
extern crate tokio_core;

use futures::Stream;
use futures::unsync::mpsc;
use tokio_core::reactor::Core;

mod group_by;
mod broadcast;
mod backlog;

#[derive(Clone, Debug)]
struct Message {
    topic_id: u32,
    text: String,
}

impl Message {
    fn new(topic_id: u32, text: &str) -> Message {
        Message {
            topic_id,
            text: text.to_string(),
        }
    }
}

fn main() {
    let mut core = Core::new().expect("failed to instantiate Core");
    let handle = core.handle();

    let (tx, rx) = mpsc::unbounded::<Message>();

    let grouped_stream = group_by::new(rx, |msg| msg.topic_id);

    let mut messages = vec![
        Message::new(1, "a"),
        Message::new(2, "b"),
        Message::new(1, "c"),
        Message::new(3, "d"),
        Message::new(2, "e"),
        Message::new(4, "f"),
    ];

    for msg in messages.drain(..) {
        let _ = mpsc::UnboundedSender::send(&tx, msg);
    }

    let f = grouped_stream.for_each(
        |stream| {
            let key = stream.key;
            println!("Received stream {}", key);

            let receiver = stream.for_each(
                move |item| {
                    println!("{} received item {:?}", key, item);
                    Ok(())
                }
            );

            handle.spawn(receiver);
            Ok(())
        }
    );

    core.run(f).unwrap();
}
