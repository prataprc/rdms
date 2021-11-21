// IMPORTANT: this module is not being used.

use std::{
    borrow::Borrow,
    sync::atomic::{AtomicPtr, Ordering::SeqCst},
};

use crate::gc::{self, Cas};

pub enum Entry<K, V> {
    S {
        next: AtomicPtr<Entry<K, V>>,
    },
    E {
        key: K,
        value: V,
        next: AtomicPtr<Entry<K, V>>,
    },
    N,
}

impl<K, V> Entry<K, V> {
    pub fn new(key: K, value: V, next: *mut Entry<K, V>) -> Entry<K, V> {
        let next = AtomicPtr::new(next);
        Entry::E { key, value, next }
    }

    pub fn update_next(&mut self, entry: *mut Entry<K, V>) {
        match self {
            Entry::S { next } => next.store(entry, SeqCst),
            Entry::E { next, .. } => next.store(entry, SeqCst),
            Entry::N => unreachable!(),
        }
    }

    pub fn cloned(&self) -> Entry<K, V>
    where
        K: Clone,
        V: Clone,
    {
        match self {
            Entry::E { key, value, next } => {
                let next = AtomicPtr::new(next.load(SeqCst));
                Entry::E {
                    key: key.clone(),
                    value: value.clone(),
                    next,
                }
            }
            Entry::S { .. } | Entry::N => unreachable!(),
        }
    }
}

impl<K, V> Entry<K, V> {
    pub fn get<Q>(head: &Entry<K, V>, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        V: Clone,
        Q: PartialEq + ?Sized,
    {
        'retry: loop {
            // head must be entry list's first sentinel, skip it.
            let mut node_ptr: *mut Entry<K, V> = head.as_next_ptr().unwrap();

            loop {
                if istagged(node_ptr) {
                    continue 'retry;
                }

                let node_ref = unsafe { node_ptr.as_ref().unwrap() };

                node_ptr = match node_ref.borrow_key::<Q>() {
                    Some(ekey) if ekey == key => {
                        break 'retry Some(node_ref.as_value().clone());
                    }
                    Some(_) => node_ref.as_next_ptr().unwrap(),
                    None => break 'retry None, // sentinel
                }
            }
        }
    }

    pub fn set(&self, nkey: &K, nvalue: &V, cas: &mut Cas<K, V>) -> Option<V>
    where
        K: PartialEq + Clone,
        V: Clone,
    {
        'retry: loop {
            // head must be entry list's first sentinel, skip it.
            let mut parent: &Entry<K, V> = self;
            let (mut node_ptr, mut next_ptr) = get_pointers(parent);

            loop {
                if istagged(next_ptr) {
                    continue 'retry;
                }

                match unsafe { node_ptr.as_ref().unwrap() } {
                    Entry::E { key, value, next } if key == nkey => {
                        let new_ptr = {
                            let next = AtomicPtr::new(next.load(SeqCst));
                            let (key, value) = (nkey.clone(), nvalue.clone());
                            let entry = Entry::E { key, value, next };
                            Box::leak(Box::new(entry))
                        };
                        cas.free_on_pass(gc::Mem::Entry(node_ptr));
                        cas.free_on_fail(gc::Mem::Entry(new_ptr));
                        if cas.swing(parent.as_atomicptr(), node_ptr, new_ptr) {
                            break 'retry Some(value.clone());
                        } else {
                            continue 'retry;
                        }
                    }
                    node_ref @ Entry::E { .. } => {
                        parent = node_ref;
                        (node_ptr, next_ptr) = get_pointers(parent);
                    }
                    Entry::S { .. } => {
                        let new_ptr = {
                            let next = AtomicPtr::new(node_ptr);
                            let (key, value) = (nkey.clone(), nvalue.clone());
                            let entry = Entry::E { key, value, next };
                            Box::leak(Box::new(entry))
                        };
                        cas.free_on_fail(gc::Mem::Entry(new_ptr));
                        if cas.swing(parent.as_atomicptr(), node_ptr, new_ptr) {
                            break 'retry None;
                        } else {
                            continue 'retry;
                        }
                    }
                    Entry::N => unreachable!(),
                }
            }
        }
    }

    pub fn remove<Q>(&self, dkey: &Q, cas: &mut Cas<K, V>) -> Option<V>
    where
        K: Borrow<Q>,
        V: Clone,
        Q: PartialEq + ?Sized,
    {
        'retry: loop {
            // head must be entry list's first sentinel, skip it.
            let mut parent: &Entry<K, V> = self;
            let (mut node_ptr, mut next_ptr) = get_pointers(parent);

            loop {
                if istagged(next_ptr) {
                    continue 'retry;
                }

                match unsafe { node_ptr.as_ref().unwrap() } {
                    Entry::E { key, value, next } if key.borrow() == dkey => {
                        // first CAS
                        let (old, new) = (next_ptr, tag(next_ptr));
                        if next.compare_and_swap(old, new, SeqCst) != old {
                            continue 'retry;
                        }
                        // second CAS
                        cas.free_on_pass(gc::Mem::Entry(node_ptr));
                        if cas.swing(parent.as_atomicptr(), node_ptr, next_ptr) {
                            break 'retry Some(value.clone());
                        } else {
                            continue 'retry;
                        }
                    }
                    node_ref @ Entry::E { .. } => {
                        parent = node_ref;
                        (node_ptr, next_ptr) = get_pointers(parent);
                    }
                    Entry::S { .. } | Entry::N => break 'retry None,
                }
            }
        }
    }
}

impl<K, V> Entry<K, V> {
    pub fn borrow_key<Q>(&self) -> Option<&Q>
    where
        K: Borrow<Q>,
        Q: ?Sized,
    {
        match self {
            Entry::E { key, .. } => Some(key.borrow()),
            Entry::S { .. } => None,
            Entry::N => unreachable!(),
        }
    }

    pub fn as_value(&self) -> &V {
        match self {
            Entry::E { value, .. } => value,
            _ => unreachable!(),
        }
    }

    fn as_next_ptr(&self) -> Option<*mut Entry<K, V>> {
        match self {
            Entry::S { next } | Entry::E { next, .. } => Some(next.load(SeqCst)),
            Entry::N => None,
        }
    }

    fn as_atomicptr(&self) -> &AtomicPtr<Entry<K, V>> {
        match self {
            Entry::S { next } | Entry::E { next, .. } => next,
            Entry::N => unreachable!(),
        }
    }
}

fn get_pointers<K, V>(parent: &Entry<K, V>) -> (*mut Entry<K, V>, *mut Entry<K, V>) {
    let node_ptr: *mut Entry<K, V> = parent.as_next_ptr().unwrap();
    let next_ptr = unsafe { node_ptr.as_ref().unwrap().as_next_ptr().unwrap() };
    (node_ptr, next_ptr)
}

fn tag<T>(ptr: *mut T) -> *mut T {
    let ptr = ptr as u64;
    assert!(ptr & 0x1 == 0);
    (ptr | 0x1) as *mut T
}

fn istagged<T>(ptr: *mut T) -> bool {
    let ptr = ptr as u64;
    (ptr & 0x1) == 1
}
