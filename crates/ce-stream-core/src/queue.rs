//! The bytes of row data waiting in the queue between an adapter's reader and
//! the consumer (issue #16). For adapters with a reader task; an adapter whose
//! consumer runs on the write path has no queue.
//!
//! The queue holds at most `queue_capacity` deliveries. With parts, one
//! delivery can be `part_max_bytes` of rows, so a slow consumer could have
//! `queue_capacity` parts waiting. The reader also waits until the deliveries
//! in the queue hold at most `queue_max_bytes` of row data; one delivery
//! always goes into an empty queue, whatever its size.

use std::sync::{Condvar, Mutex};

use crate::event::approx_json_bytes;
use crate::transaction::CommittedTransaction;

/// A source's tasks end with its future, which a consumer can drop at any await (a
/// `select!`): a reader that went on would hold the connection, and a replication
/// slot, until the process ends.
pub struct ReaderTasks {
    queue: std::sync::Arc<QueueBytes>,
    tasks: Vec<tokio::task::AbortHandle>,
}

impl ReaderTasks {
    pub fn new(queue: std::sync::Arc<QueueBytes>, tasks: Vec<tokio::task::AbortHandle>) -> Self {
        Self { queue, tasks }
    }
}

impl Drop for ReaderTasks {
    fn drop(&mut self) {
        self.queue.close();
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// The row data of a delivery, as a part is measured.
pub fn delivery_bytes(txn: &CommittedTransaction) -> u64 {
    txn.events
        .iter()
        .map(|event| approx_json_bytes(&event.data) as u64)
        .sum()
}

#[derive(Debug)]
pub struct QueueBytes {
    max: u64,
    /// Bytes in the queue, and whether the consumer is gone.
    state: Mutex<(u64, bool)>,
    room: Condvar,
}

impl QueueBytes {
    pub fn new(max: u64) -> Self {
        Self {
            max: max.max(1),
            state: Mutex::new((0, false)),
            room: Condvar::new(),
        }
    }

    /// Wait until a delivery of `bytes` fits, and count it in. An error when
    /// the consumer is gone.
    pub fn reserve(&self, bytes: u64) -> Result<(), String> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if state.1 {
                return Err("event channel closed".into());
            }
            if state.0 == 0 || state.0.saturating_add(bytes) <= self.max {
                state.0 = state.0.saturating_add(bytes);
                return Ok(());
            }
            state = self.room.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// The consumer is done with a delivery of `bytes`.
    pub fn release(&self, bytes: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.0 = state.0.saturating_sub(bytes);
        self.room.notify_all();
    }

    /// The consumer is gone: a reader that waits gets an error.
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.1 = true;
        self.room.notify_all();
    }

    #[cfg(test)]
    fn queued(&self) -> u64 {
        self.state.lock().unwrap().0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn the_reader_waits_until_the_queue_has_room() {
        let queue = Arc::new(QueueBytes::new(100));
        queue.reserve(60).unwrap();
        queue.reserve(40).unwrap();
        let waiting = Arc::clone(&queue);
        let reader = std::thread::spawn(move || waiting.reserve(30));
        std::thread::sleep(Duration::from_millis(100));
        assert!(!reader.is_finished(), "130 bytes do not fit in 100");
        queue.release(60);
        reader.join().unwrap().unwrap();
        assert_eq!(queue.queued(), 70);
    }

    #[test]
    fn one_delivery_always_fits_an_empty_queue() {
        let queue = QueueBytes::new(100);
        queue.reserve(500).unwrap();
        assert_eq!(queue.queued(), 500);
    }

    #[test]
    fn a_waiting_reader_ends_when_the_consumer_is_gone() {
        let queue = Arc::new(QueueBytes::new(10));
        queue.reserve(10).unwrap();
        let waiting = Arc::clone(&queue);
        let reader = std::thread::spawn(move || waiting.reserve(10));
        std::thread::sleep(Duration::from_millis(50));
        queue.close();
        assert!(reader.join().unwrap().is_err());
    }
}
