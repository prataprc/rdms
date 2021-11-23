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

    pub fn into_key(self) -> K {
        match self {
            Access::N { key, .. } => key,
            _ => unreachable!(),
        }
    }

    pub fn get_next(&self) -> &Access<K> {
        match self {
            Access::T { next } => unsafe { next.load(SeqCst).as_ref().unwrap() },
            Access::N { next, .. } => unsafe { next.load(SeqCst).as_ref().unwrap() },
            Access::H { .. } => unreachable!(),
        }
    }

    pub fn delete_next(&self) -> K {
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

        node.into_key()
    }

    /// Append to head of the list.
    pub fn append<Q>(&self, key: &Q) -> *mut Access<K>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + PartialEq + ?Sized,
    {
        let head_ptr = self as *const Access<K> as *mut Access<K>;
        // self is head !!
        loop {
            let node = Box::new(Access::N {
                key: key.to_owned(),
                born: time::Instant::now(),
                deleted: AtomicBool::new(false),
                next: AtomicPtr::new(head_ptr),
            });
            let new = Box::leak(node);

            match self {
                Access::H { prev } => {
                    let old = prev.load(SeqCst);
                    match prev.compare_exchange(old, new, SeqCst, SeqCst) {
                        Ok(_) => {
                            let next = match unsafe { old.as_ref().unwrap() } {
                                Access::T { next } => next,
                                Access::N { next, .. } => next,
                                _ => unreachable!(),
                            };
                            match next.compare_exchange(head_ptr, new, SeqCst, SeqCst) {
                                Ok(_) => break new,
                                Err(_) => unreachable!(),
                            }
                        }
                        Err(_) => {
                            let _node = unsafe { Box::from_raw(new) };
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    /// Mark the node as deleted.
    pub fn delete(&self) {
        match self {
            Access::N { deleted, .. } => deleted.store(true, SeqCst),
            _ => unreachable!(),
        }
    }
}
