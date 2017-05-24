use std::mem;
use std::sync::Arc;

struct Backlog<T> {
    buffer: Vec<T>,
    revision: u32,
    next_index: usize,
    snapshot: BacklogSnapshot<T>,
}

#[derive(Clone)]
struct BacklogSnapshot<T> {
    buffer: Arc<Vec<T>>,
    revision: u32,
}

impl<T: Clone> Backlog<T> {
    fn with_capacity(capacity: usize) -> Self {
        Backlog {
            buffer: Vec::with_capacity(capacity),
            revision: 0,
            next_index: 0,
            snapshot: BacklogSnapshot {
                buffer: Arc::new(Vec::with_capacity(0)),
                revision: 0,
            },
        }
    }

    fn insert(&mut self, item: T) {
        if self.buffer.len() < self.buffer.capacity() {
            self.buffer.push(item);
        } else {
            self.buffer.insert(self.next_index, item);
        }
        self.next_index = (self.next_index + 1) % self.buffer.capacity();
        self.revision.wrapping_add(1);
    }

    fn snapshot(&mut self) -> Arc<Vec<T>> {
        if self.snapshot.revision == self.revision {
            self.snapshot.buffer.clone()
        } else {
            let (left, right) = self.buffer.split_at(self.next_index);

            let mut items = Vec::with_capacity(self.buffer.len());

            items.extend(right.into_iter().cloned());
            items.extend(left.into_iter().cloned());

            self.snapshot.buffer = Arc::new(items);
            self.snapshot.revision = self.revision;

            self.snapshot.buffer.clone()
        }
    }
}
