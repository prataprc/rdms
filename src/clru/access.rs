// Module implement the single linked list of access time.
//
// For every cache-hit and a new cache-insert, we prepend the key into the access list
// and invalidate the older node in the list (in case of cache-hit, for cache-insert
// there is no older node)

use std::{
    sync::{
        atomic::{AtomicBool, AtomicPtr, Ordering::SeqCst},
        Arc,
    },
    time::{self, Duration},
};

// S - Sentinel
// T - Access time-stamp
// N - None
pub enum Access<K> {
    S {
        next: AtomicPtr<Access<K>>,
    },
    T {
        key: K,
        born: Duration, // elapsed time in uS since UNIX_EPOCH.
        deleted: AtomicBool,
        next: Option<Box<Access<K>>>,
    },
    N,
}

impl<K> Access<K> {
    /// Create a new access-list.
    ///
    /// An empty access list is maintained as Access::S.next -> Access::N. That is,
    /// a sentinel node pointing to end-node.
    pub fn new_list() -> Arc<Access<K>> {
        Arc::new(Access::S {
            next: AtomicPtr::new(Box::leak(Box::new(Access::N))),
        })
    }

    /// Create a new access-node, always of type Access::T. Subsequently the list
    /// will be maintained as,
    ///
    /// Access::S.next -> Access::T.next -> ... -> Access::N
    pub fn new(key: K) -> Box<Access<K>> {
        Box::new(Access::T {
            key,
            born: time::UNIX_EPOCH.elapsed().unwrap(),
            deleted: AtomicBool::new(false),
            next: None,
        })
    }

    /// Mark the node as deleted.
    ///
    /// Note that evictor will remove an access node if,
    /// * Node is marked as deleted.
    /// * Number of nodes in the access list exceeds the cache-limit. In this case,
    ///   evictor shall first deleted the entry from the cache before removing it
    ///   from the list.
    pub fn delete(&self) {
        match self {
            Access::T { deleted, .. } => deleted.store(true, SeqCst),
            _ => unreachable!(),
        }
    }

    pub fn get_next_mut(&mut self) -> &mut Access<K> {
        match self {
            Access::S { next } => unsafe { next.load(SeqCst).as_mut().unwrap() },
            Access::T { next, .. } => next.as_mut().unwrap(),
            Access::N => unreachable!(),
        }
    }

    pub fn set_next(&mut self, access: Box<Access<K>>) {
        match self {
            Access::T { next, .. } => *next = Some(access),
            _ => unreachable!(),
        }
    }

    pub fn take_next(&mut self) -> Box<Access<K>> {
        match self {
            Access::T { next, .. } => next.take().unwrap(),
            _ => unreachable!(),
        }
    }

    pub fn prepend(&self, mut node: Box<Access<K>>) {
        loop {
            match self {
                Access::S { next } => {
                    let old = next.load(SeqCst);
                    node.set_next(unsafe { Box::from_raw(old) });
                    let new = Box::leak(node);
                    match next.compare_exchange(old, new, SeqCst, SeqCst) {
                        Ok(_) => break,
                        Err(_) => {
                            node = unsafe { Box::from_raw(new) };
                            Box::leak(node.take_next());
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}
