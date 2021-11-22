use cmap::Map;

use std::{
    hash::{BuildHasher, Hash},
    sync::{
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc,
    },
    time::{self, Duration},
};

use crate::{
    clru::{self, Access},
    Result,
};

pub struct Writer<K, V, H>
where
    K: Clone + PartialEq + Hash,
    V: Clone,
    H: BuildHasher,
{
    map: Map<K, clru::Value<K, V>, H>,
    tx: mpsc::
}

impl<K, V, H> Evictor<K, V, H>
where
    K: Clone + PartialEq + Hash,
    V: Clone,
    H: BuildHasher,
{
    /// Create a new evictor,
    ///
    /// max_count: evict nodes to keep them under max_count.
    /// max_old: nodes older than max_old shall be evicted.
    /// close: sync data to signal that map invalidated and access_list can be dropped.
    pub fn new(
        max_count: usize,
        map: Map<K, clru::Value<K, V>, H>,
        close: Arc<AtomicBool>,
        access_list: Arc<Access<K>>,
    ) -> Self {
        Evictor {
            max_count,
            max_old: time::UNIX_EPOCH.elapsed().unwrap(),
            map,
            close,
            access_list,
        }
    }

    pub fn set_max_old(&mut self, max_old: Duration) -> &mut Self {
        self.max_old = max_old;
        self
    }
}

impl<K, V, H> Evictor<K, V, H>
where
    K: Clone + PartialEq + Hash,
    V: Clone,
    H: BuildHasher,
{
    pub fn run(mut self) -> Result<()> {
        loop {
            if self.close.load(SeqCst) {
                break;
            }

            // initialize vars for this iteration.
            self.do_eviction().ok(); // TODO: is it okay to ignore this error
        }

        let _node: Box<Access<K>> = match self.access_list.as_ref() {
            Access::S { next } => unsafe { Box::from_raw(next.load(SeqCst)) },
            _ => unreachable!(),
        };

        // _node drop the entire chain of access list.

        Ok(())
    }

    fn do_eviction(&mut self) -> Result<()> {
        let mut count = 0;
        let mut evict = false;
        let epoch = time::UNIX_EPOCH.elapsed().unwrap() - self.max_old;

        // skip the sentinel.
        let mut node: &mut Access<K> = match self.access_list.as_ref() {
            Access::S { next } => unsafe { next.load(SeqCst).as_mut().unwrap() },
            _ => unreachable!(),
        };
        // iterate on the access-list.
        loop {
            evict = evict || count > self.max_count;
            node = match *node.take_next() {
                Access::T { next, deleted, .. } if deleted.load(SeqCst) => {
                    node.set_next(next.unwrap());
                    node.get_next_mut()
                }
                Access::T {
                    key, born, next, ..
                } if evict || born < epoch => {
                    // IMPORTANT: Don't change the following sequence.
                    self.map.remove(&key);
                    node.set_next(next.unwrap());
                    node.get_next_mut()
                }
                Access::T { .. } => {
                    count += 1;
                    node.get_next_mut()
                }
                Access::N => break,
                _ => unreachable!(),
            }
        }

        Ok(())
    }
}
