/// RWSpinlock implements the idea of latch-and-spin mechanism normally
/// used for non-blocking concurrency.
///
/// Blocking concurrency can have impact on latency. On the other hand,
/// when the operations are going to be quick and short, we can use
/// non-blocking primitives like latch-and-spin.
///
/// **What is Latch and spin ?**
///
/// In typical multi-core processors, concurrent read operations are
/// always safe and consistent. But it becomes unsafe, when there is
/// a writer concurrently modifying data while readers are loading it
/// from memory.
///
/// Latch-and-lock mechanism can be used when we want to allow
/// one or more concurrent writer(s) along with readers.
///
/// Imagine a door leading into a room. This door has some special
/// properties:
/// a. The door has a latch-and a spin-lock.
/// b. A _READER_ can enter the room, only when the door is unlocked
///    and unlatched.
/// c. A _WRITER_ can enter the room, only when the door is unlocked
///    and, unlatched and, there are no other reader or writer in the
///    room.
/// d. Once the door is latched by a writer, no other writer or reader
///    can enter the room (because of (a) and (b) properties). But any
///    readers who are already inside the room, can exit.
/// e. A writer can enter the room only after locking the door, which
///    the writer can do only after all the readers have exited.
/// f. When trying to acquire read-permission or write-permission, the
///    caller thread shall spin until all the conditions from (a) to (e)
///    is met.
///
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fmt, thread};

/// RWSpinlock implements latch-and-spin mechanism for non-blocking
/// concurrency.
///
/// It uses AtomicU64 for:
/// a. ref-count, bits [0-61].
/// b. latch flag, bit 62.
/// c. lock flag, bit 63.
///
/// All atomic operations use Ordering::Relaxed.
///
pub struct RWSpinlock {
    value: AtomicU64,
    reads: AtomicU64,
    writes: AtomicU64,
    conflicts: AtomicU64,
}

impl RWSpinlock {
    const LATCH_FLAG: u64 = 0x4000000000000000;
    const LOCK_FLAG: u64 = 0x8000000000000000;
    const LATCH_LOCK_FLAG: u64 = 0xC000000000000000;
    const READERS_FLAG: u64 = 0x3FFFFFFFFFFFFFFF;

    /// Create a new RWSpinlock
    pub fn new() -> RWSpinlock {
        RWSpinlock {
            value: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            conflicts: AtomicU64::new(0),
        }
    }

    /// Acquire latch for read permission. If yield_ok is true, calling
    /// thread will yield to scheduler before re-trying the latch.
    pub fn acquire_read(&self, yield_ok: bool) -> Reader {
        loop {
            let c = self.value.load(Ordering::Relaxed);
            if (c & Self::LATCH_LOCK_FLAG) == 0 {
                // latch is not acquired by a writer
                let n = c + 1;
                if self.value.compare_and_swap(c, n, Ordering::Relaxed) == c {
                    self.reads.fetch_add(1, Ordering::Relaxed);
                    break Reader { door: self };
                }
            }
            self.conflicts.fetch_add(1, Ordering::Relaxed);
            if yield_ok {
                thread::yield_now();
            }
        }
    }

    /// Acquire latch for write permission. If yield_ok is true, calling
    /// thread will yield to scheduler before re-trying the latch.
    pub fn acquire_write(&self, yield_ok: bool) -> Writer {
        // acquire latch
        loop {
            let c = self.value.load(Ordering::Relaxed);
            if (c & Self::LATCH_FLAG) == 0 {
                // latch is not acquired by a writer
                if (c & Self::LOCK_FLAG) != 0 {
                    panic!("if latch is flipped-off, lock can't be flipped-on !");
                }
                let n = c | Self::LATCH_FLAG;
                if self.value.compare_and_swap(c, n, Ordering::Relaxed) == c {
                    break;
                }
            }
            self.conflicts.fetch_add(1, Ordering::Relaxed);
            if yield_ok {
                thread::yield_now();
            }
        }
        // acquire lock
        loop {
            let c = self.value.load(Ordering::Relaxed);
            if (c & Self::READERS_FLAG) == 0 {
                let n = c | Self::LOCK_FLAG;
                if self.value.compare_and_swap(c, n, Ordering::Relaxed) == c {
                    self.writes.fetch_add(1, Ordering::Relaxed);
                    break Writer { door: self };
                }
                panic!("latch is acquired, ZERO readers, but unable to lock !")
            }
            self.conflicts.fetch_add(1, Ordering::Relaxed);
            if yield_ok {
                thread::yield_now();
            }
        }
    }
}

pub struct Reader<'a> {
    door: &'a RWSpinlock,
}

impl<'a> Drop for Reader<'a> {
    fn drop(&mut self) {
        self.door.value.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct Writer<'a> {
    door: &'a RWSpinlock,
}

impl<'a> Drop for Writer<'a> {
    fn drop(&mut self) {
        let c = self.door.value.load(Ordering::Relaxed);
        if (c & RWSpinlock::READERS_FLAG) > 0 {
            panic!("can't have active readers, when lock is held");
        }
        if self.door.value.compare_and_swap(c, 0, Ordering::Relaxed) != c {
            panic!("cant' have readers/writers to modify when locked")
        }
    }
}

impl fmt::Display for RWSpinlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "value:{:X} rs:{} ws:{} cs:{}",
            self.value.load(Ordering::Relaxed),
            self.reads.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
            self.conflicts.load(Ordering::Relaxed),
        )
    }
}
