// Module implement the single linked list of access time.
//
// For every cache-hit and a new cache-insert, we prepend the key into the access list
// and invalidate the older node in the list (in case of cache-hit, for cache-insert
// there is no older node)

use std::{
    borrow::Borrow,
    sync::{
        atomic::{AtomicBool, AtomicPtr, Ordering::SeqCst},
        Arc,
    },
    time,
};

pub enum Access<K> {
    T {
        next: AtomicPtr<Access<K>>,
    },
    N {
        key: K,
        born: time::Instant,
        deleted: AtomicBool,
        epoch: u64,
        next: AtomicPtr<Access<K>>,
    },
    H {
        prev: AtomicPtr<Access<K>>,
    },
}

impl<K> Access<K> {
    /// Create a new access-list. An empty access list is maintained as,
    /// `Access::H.prev -> `Access::T`
    /// `Access::T.next -> `Access::H`
    ///
    /// return (head, tail)
    pub fn new_list() -> (Arc<Access<K>>, Arc<Access<K>>) {
        // create head
        let head = Arc::new(Access::H {
            prev: AtomicPtr::new(std::ptr::null::<Access<K>>() as *mut Access<K>),
        });
        // create tail
        let tail = Arc::new(Access::T {
            next: AtomicPtr::new(Arc::as_ptr(&head) as *mut Access<K>),
        });
        // link them back to back
        match head.as_ref() {
            Access::H { prev } => {
                prev.store(Arc::as_ptr(&tail) as *mut Access<K>, SeqCst)
            }
            _ => unreachable!(),
        };

        (head, tail)
    }

    pub fn new<Q>(&self, key: &Q) -> Box<Self>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + PartialEq + ?Sized,
    {
        let head_ptr = self as *const Access<K> as *mut Access<K>;

        let val = Access::N {
            key: key.to_owned(),
            born: time::Instant::now(),
            deleted: AtomicBool::new(false),
            epoch: u64::MAX,
            next: AtomicPtr::new(head_ptr),
        };

        Box::new(val)
    }

    pub fn as_key(&self) -> &K {
        match self {
            Access::N { key, .. } => key,
            _ => unreachable!(),
        }
    }

    pub fn next_key(&self) -> &K {
        let next = match self {
            Access::T { next } => unsafe { next.load(SeqCst).as_ref().unwrap() },
            Access::N { next, .. } => unsafe { next.load(SeqCst).as_ref().unwrap() },
            _ => unreachable!(),
        };
        next.as_key()
    }

    pub fn get_next(&self) -> &Access<K> {
        match self {
            Access::T { next } => unsafe { next.load(SeqCst).as_ref().unwrap() },
            Access::N { next, .. } => unsafe { next.load(SeqCst).as_ref().unwrap() },
            Access::H { .. } => unreachable!(),
        }
    }

    pub fn delete_next(&self) -> Box<Access<K>> {
        let next = match self {
            Access::T { next } => next,
            Access::N { next, .. } => next,
            _ => unreachable!(),
        };
        let node = unsafe { Box::from_raw(next.load(SeqCst)) };
        next.store(
            node.get_next() as *const Access<K> as *mut Access<K>,
            SeqCst,
        );

        node
    }

    pub fn set_epoch(&mut self, seqno: u64) {
        match self {
            Access::N { epoch, .. } => *epoch = seqno,
            _ => unreachable!(),
        }
    }

    #[allow(dead_code)]
    pub fn to_epoch(&self) -> u64 {
        match self {
            Access::N { epoch, .. } => *epoch,
            _ => unreachable!(),
        }
    }

    #[allow(dead_code)]
    pub fn is_deleted(&self) -> bool {
        match self {
            Access::N { deleted, .. } => deleted.load(SeqCst),
            _ => unreachable!(),
        }
    }

    /// Append to head of the list.
    pub fn append(&self, node: Box<Access<K>>) {
        let head_ptr = self as *const Access<K> as *mut Access<K>;
        let new = Box::leak(node);

        // self is head !!
        loop {
            match self {
                Access::H { prev } => {
                    let old = prev.load(SeqCst);
                    // println!("append new access old:{:p} new:{:p}", old, new);
                    match prev.compare_exchange(old, new, SeqCst, SeqCst) {
                        Ok(_) => {
                            let next = match unsafe { old.as_ref().unwrap() } {
                                Access::T { next } => next,
                                Access::N { next, .. } => next,
                                _ => unreachable!(),
                            };
                            match next.compare_exchange(head_ptr, new, SeqCst, SeqCst) {
                                Ok(_) => break,
                                Err(_) => unreachable!(),
                            }
                        }
                        Err(_) => {
                            #[cfg(feature = "debug")]
                            println!("access append loop {:p}", new);
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    /// Mark the node as deleted.
    #[allow(unreachable_patterns)]
    pub fn delete(&self) {
        // println!("delete access {:p}", self);
        match self {
            Access::N { deleted, .. } => deleted.store(true, SeqCst),
            Access::T { .. } => unreachable!(),
            Access::H { .. } => unreachable!(),
            _ => unreachable!(),
        }
    }
}
