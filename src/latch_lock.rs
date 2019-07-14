// TODO: measure performance by using std::thread::yield_now()

use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct LatchLock(AtomicU64);

impl LatchLock {
    const LATCH_FLAG: u64 = 0x4000000000000000;
    const LOCK_FLAG: u64 = 0x8000000000000000;
    const READERS_FLAG: u64 = 0x3FFFFFFFFFFFFFFF;

    fn new() -> LatchLock {
        LatchLock(AtomicU64::new(0))
    }

    fn new_reader(&self) -> Reader {
        loop {
            let mut c = self.0.load(Ordering::Relaxed);
            if (c & Self::LATCH_FLAG) != 0 {
                // latch is acquired by a writer
                continue;
            }
            let n = c + 1;
            if self.0.compare_and_swap(c, n, Ordering::Relaxed) != c {
                // unlucky, a concurrent thread has modified this latch-lock
                continue;
            }
            break Reader { door: self };
        }
    }

    fn new_writer(&self) -> Writer {
        // acquire latch
        loop {
            let c = self.0.load(Ordering::Relaxed);
            if (c & Self::LATCH_FLAG) != 0 {
                // latch is acquired by a writer
                continue;
            }
            if (c & Self::LOCK_FLAG) != 0 {
                panic!("if latch is flipped-off, lock can't be found-on !");
            }
            let n = c | Self::LATCH_FLAG;
            if self.0.compare_and_swap(c, n, Ordering::Relaxed) != c {
                // another concurrent thread has modified this latch-lock
                continue;
            }
            break;
        }
        // acquire lock
        loop {
            let c = self.0.load(Ordering::Relaxed);
            if (c & Self::READERS_FLAG) > 0 {
                continue;
            }
            let n = c | Self::LOCK_FLAG;
            if self.0.compare_and_swap(c, n, Ordering::Relaxed) != c {
                panic!("latch is acquired, active readers exited, but locked !")
            }
            break Writer { door: self };
        }
    }
}

struct Reader<'a> {
    door: &'a LatchLock,
}

impl<'a> Drop for Reader<'a> {
    fn drop(&mut self) {
        self.door.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Writer<'a> {
    door: &'a LatchLock,
}

impl<'a> Drop for Writer<'a> {
    fn drop(&mut self) {
        let c = self.door.0.load(Ordering::Relaxed);
        if (c & LatchLock::READERS_FLAG) > 0 {
            panic!("can't have active readers, when lock is held");
        }
        self.door.0.compare_and_swap(c, 0, Ordering::Relaxed);
    }
}
