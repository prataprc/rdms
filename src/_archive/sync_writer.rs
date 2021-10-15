use std::sync::atomic::{
    AtomicU64,
    Ordering::{AcqRel, Acquire},
};

/// SyncWriter is used to make sure that only one writer is going to
/// access MVCC index. Calling lock() from more than one thread will
/// cause panic. It is better to deligate all write operations to
/// single thread as opposed to serializing the write operations from
/// multiple threads.
#[allow(dead_code)] // TODO: do we really need this type for rdms ??
pub(crate) struct SyncWriter {
    writers: AtomicU64,
}

#[allow(dead_code)]
impl SyncWriter {
    pub(crate) fn new() -> SyncWriter {
        SyncWriter {
            writers: AtomicU64::new(0),
        }
    }

    pub(crate) fn lock<'a>(&'a self) -> Fence<'a> {
        if self.writers.compare_exchange(0, 1, AcqRel, Acquire) != Ok(0) {
            panic!("Mvcc cannot have concurrent writers");
        }
        Fence { fence: self }
    }
}

pub(crate) struct Fence<'a> {
    fence: &'a SyncWriter,
}

impl<'a> Drop for Fence<'a> {
    fn drop(&mut self) {
        if self.fence.writers.compare_exchange(1, 0, AcqRel, Acquire) != Ok(1) {
            panic!("unepxected situation in spinlock drop");
        }
    }
}

#[cfg(test)]
#[path = "sync_writer_test.rs"]
mod sync_writer_test;
