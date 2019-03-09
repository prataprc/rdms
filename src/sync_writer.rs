use std::sync::atomic::{AtomicU64, Ordering};

pub struct SyncWriter {
    writers: AtomicU64,
}

impl SyncWriter {
    pub fn new() -> SyncWriter {
        SyncWriter {
            writers: AtomicU64::new(0),
        }
    }

    pub fn lock<'a>(&'a self) -> Fence<'a> {
        if self.writers.compare_and_swap(0, 1, Ordering::Relaxed) != 0 {
            panic!("Mvcc cannot have concurrent writers");
        }
        Fence { fence: self }
    }
}

pub struct Fence<'a> {
    fence: &'a SyncWriter,
}

impl<'a> Drop for Fence<'a> {
    fn drop(&mut self) {
        if self.fence.writers.compare_and_swap(1, 0, Ordering::Relaxed) != 1 {
            unreachable!();
        }
    }
}