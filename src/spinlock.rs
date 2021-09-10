//! Module `spinlock` implement read-write-spinlock, useful for
//! non-blocking concurrency.
//!
//! Blocking concurrency can have impact on latency. When operations
//! that require [rw-exclusion][rw-lock] is going to be quick and short,
//! we can use non-blocking primitives like latch-and-spin.
//!
//! **What is Latch and spin ?**
//!
//! In typical multi-core processors, concurrent read operations are
//! always safe and consistent. But it becomes unsafe, when there is a
//! writer concurrently modifying data while readers are loading it from
//! memory.
//!
//! Latch-and-lock mechanism can be used when we want to allow one or
//! more concurrent writer(s) along with readers.
//!
//! Imagine a door leading into a room. This door has some special
//! properties:
//!
//! 1. The door has a latch and a lock.
//! 2. A **reader** can enter the room only when the door is un-locked
//!    and un-latched.
//! 3. A **writer** can enter the room only when the door is un-locked
//!    and, un-latched and, there are no other **reader** or **writer**
//!    in the room.
//! 4. Once the door is latched by a **writer**, no other **writer**
//!    or **reader** can enter the room because of (1) and (2) properties.
//!    But all **readers** who are already inside the room can finish
//!    their job and then exit.
//! 5. A **writer** can enter the room only after locking the door, which
//!    the **writer** can do only after all the **readers** have exited.
//! 6. When trying to acquire read-permission or write-permission, the
//!    caller thread shall spin until all the conditions from (1) to (5)
//!    are met. Once a thread acquires necessary permission it can
//!    continue to finish its job and then release the permission.
//!
//! [rw-lock]: https://en.wikipedia.org/wiki/Readers–writer_lock
//!

use std::{
    convert::TryInto,
    fmt, result,
    sync::atomic::{
        AtomicU64,
        Ordering::{Acquire, SeqCst},
    },
    thread,
};

use crate::{
    core::{Result, ToJson},
    error::Error,
};

// TODO: Experiment with different atomic::Ordering to improve performance.

/// RWSpinlock implements latch-and-spin mechanism for non-blocking
/// concurrency.
///
/// It uses AtomicU64 for:
/// * ref-count, bits [0-61].
/// * latch flag, bit 62.
/// * lock flag, bit 63.
pub struct RWSpinlock {
    value: AtomicU64,
    read_locks: AtomicU64,
    write_locks: AtomicU64,
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
            read_locks: AtomicU64::new(0),
            write_locks: AtomicU64::new(0),
            conflicts: AtomicU64::new(0),
        }
    }

    /// Acquire latch for read permission. If ``spin`` is false, calling
    /// thread will yield to scheduler before re-trying the latch.
    pub fn acquire_read(&self, spin: bool) -> Reader {
        loop {
            let c = self.value.load(SeqCst);
            if (c & Self::LATCH_LOCK_FLAG) == 0 {
                // latch is not acquired by a writer
                let n = c + 1;
                if self.value.compare_exchange(c, n, SeqCst, Acquire) == Ok(c) {
                    self.read_locks.fetch_add(1, SeqCst);
                    break Reader { door: self };
                }
            }
            self.conflicts.fetch_add(1, SeqCst);
            if !spin {
                thread::yield_now();
            }
        }
    }

    /// Acquire latch for write permission. If ``spin`` is false, calling
    /// thread will yield to scheduler before re-trying the latch.
    pub fn acquire_write(&self, spin: bool) -> Writer {
        // acquire latch
        loop {
            let c = self.value.load(SeqCst);
            if (c & Self::LATCH_FLAG) == 0 {
                // latch is not acquired by a writer
                if (c & Self::LOCK_FLAG) != 0 {
                    panic!("if latch is flipped-off, lock can't be flipped-on !");
                }
                let n = c | Self::LATCH_FLAG;
                if self.value.compare_exchange(c, n, SeqCst, Acquire) == Ok(c) {
                    break;
                }
            }
            self.conflicts.fetch_add(1, SeqCst);
            if !spin {
                thread::yield_now();
            }
        }
        // acquire lock
        loop {
            let c = self.value.load(SeqCst);
            if (c & Self::READERS_FLAG) == 0 {
                let n = c | Self::LOCK_FLAG;
                if self.value.compare_exchange(c, n, SeqCst, Acquire) == Ok(c) {
                    self.write_locks.fetch_add(1, SeqCst);
                    break Writer { door: self };
                }
                panic!("latch is acquired, ZERO readers, but unable to lock !")
            }
            self.conflicts.fetch_add(1, SeqCst);
            if !spin {
                thread::yield_now();
            }
        }
    }

    pub fn to_stats(&self) -> Result<Stats> {
        Ok(Stats {
            value: self.value.load(SeqCst),
            read_locks: convert_at!(self.read_locks.load(SeqCst))?,
            write_locks: convert_at!(self.write_locks.load(SeqCst))?,
            conflicts: convert_at!(self.conflicts.load(SeqCst))?,
        })
    }
}

/// Type to handle read-latch, when value gets dropped the latch is released.
pub struct Reader<'a> {
    door: &'a RWSpinlock,
}

impl<'a> Drop for Reader<'a> {
    fn drop(&mut self) {
        self.door.value.fetch_sub(1, SeqCst);
    }
}

/// Type to handle write-latch, when value gets dropped the latch is released.
pub struct Writer<'a> {
    door: &'a RWSpinlock,
}

impl<'a> Drop for Writer<'a> {
    fn drop(&mut self) {
        let c = self.door.value.load(SeqCst);
        if (c & RWSpinlock::READERS_FLAG) > 0 {
            panic!("can't have active readers, when lock is held");
        }
        if self.door.value.compare_exchange(c, 0, SeqCst, Acquire) != Ok(c) {
            panic!("cant' have readers/writers to modify when locked")
        }
    }
}

/// Statistic type, to capture [RWSpinlock] metrics.
#[derive(Default)]
pub struct Stats {
    /// Actual 64-bit value of the RWSpinlock when
    /// [to_stats][RWSpinlock::to_stats] is called.
    pub value: u64,
    /// Total number of read locks so far.
    pub read_locks: usize,
    /// Total number of write locks so far.
    pub write_locks: usize,
    /// Total number of conflicts so far, while acquire the latch.
    pub conflicts: usize,
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(
            f,
            concat!(
                "{{ value = {:X}, read_locks = {}, ",
                "write_locks = {}, conflicts = {} }}",
            ),
            self.value, self.read_locks, self.write_locks, self.conflicts,
        )
    }
}

impl ToJson for Stats {
    fn to_json(&self) -> String {
        format!(
            concat!(
                r#"{{ "value": {:X}, "read_locks": {}, "#,
                r#""write_locks": {}, "conflicts": {} }}"#,
            ),
            self.value, self.read_locks, self.write_locks, self.conflicts
        )
    }
}

#[cfg(test)]
#[path = "spinlock_test.rs"]
mod spinlock_test;
