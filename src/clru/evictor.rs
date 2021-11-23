use std::{
    hash::{BuildHasher, Hash},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    time,
};

use crate::{
    clru::{self, Access, Config},
    dbs::{self, Footprint},
    Result,
};

/// Note that evictor will remove an access node if,
///
/// * Node is marked as deleted.
/// * Node is older than configured elapsed time, optional.
/// * Number of nodes in the access list exceed the count-limit, optional.
/// * Cummulative size of values held in cache exceeds size-limit, optional.
pub struct Evictor<K, V, H> {
    max_size: Option<usize>,
    cur_size: Option<Arc<AtomicUsize>>,
    max_count: usize,
    cur_count: Arc<AtomicUsize>,
    max_old: Option<time::Duration>,

    map: cmap::Map<K, clru::Value<K, V>, H>,
    access_tail: Arc<Access<K>>,
    close: Arc<AtomicBool>,

    pub(crate) n_evicted: usize,
    pub(crate) n_deleted: usize,
}

enum Evict {
    Deleted,
    Ok,
    None,
}

impl<K, V, H> Evictor<K, V, H> {
    /// Create a new evictor.
    pub fn new(
        config: &Config,
        close: Arc<AtomicBool>,
        access_tail: Arc<Access<K>>,
        map: cmap::Map<K, clru::Value<K, V>, H>,
    ) -> Self {
        Evictor {
            max_size: config.max_size.clone(),
            cur_size: config.cur_size.clone(),
            max_count: config.max_count,
            cur_count: Arc::clone(&config.cur_count),
            max_old: config.max_old.map(time::Duration::from_secs),

            map,
            access_tail,
            close,

            n_evicted: 0,
            n_deleted: 0,
        }
    }
}

impl<K, V, H> Evictor<K, V, H>
where
    K: Clone + PartialEq + Hash,
    V: Clone + dbs::Footprint,
    H: BuildHasher,
{
    pub fn run(mut self) -> Result<Self> {
        loop {
            if self.close.load(SeqCst) {
                break;
            }
            self.do_eviction()?;
        }

        Ok(self)
    }

    fn to_evict(&self, node: &Access<K>) -> Evict {
        match node {
            Access::N { deleted, born, .. } => {
                match deleted.load(SeqCst) {
                    true => return Evict::Deleted,
                    false => (),
                }

                let cur_size = self.cur_size.as_ref().map(|x| x.load(SeqCst));
                let mut evicta = match cur_size {
                    Some(cur_size) if cur_size > self.max_size.unwrap() => true,
                    Some(_) | None => false,
                };

                evicta = evicta || self.cur_count.load(SeqCst) > self.max_count;

                evicta = evicta
                    || match self.max_old.clone() {
                        Some(max_old) if born.elapsed() > max_old => true,
                        Some(_) | None => false,
                    };

                match evicta {
                    true => Evict::Ok,
                    false => Evict::None,
                }
            }
            _ => unreachable!(),
        }
    }

    fn do_eviction(&mut self) -> Result<()> {
        // delay-pointer iteration
        let mut n = 5;
        let mut behind: &Access<K> = self.access_tail.as_ref();

        let mut ahead: Option<&Access<K>> = Some(behind);
        let ahead: Option<&Access<K>> = loop {
            ahead = match ahead {
                Some(Access::N { next, .. }) if n > 0 => {
                    n -= 1;
                    Some(unsafe { next.load(SeqCst).as_ref().unwrap() })
                }
                Some(Access::N { .. }) => break ahead,
                Some(Access::T { next }) => {
                    Some(unsafe { next.load(SeqCst).as_ref().unwrap() })
                }
                Some(Access::H { .. }) | None => break None,
            }
        };

        match ahead {
            Some(mut ahead) => loop {
                ahead = match ahead {
                    Access::N { next, .. } => {
                        behind = match self.to_evict(behind.get_next()) {
                            Evict::Deleted => {
                                self.n_deleted += 1;
                                let _key = behind.delete_next();
                                behind
                            }
                            Evict::Ok => {
                                self.n_evicted += 1;
                                let key = behind.delete_next();
                                let size = match self.map.remove(&key) {
                                    Some(value) => value.footprint()?,
                                    None => 0,
                                };
                                self.cur_size.as_ref().map(|x| {
                                    if size < 0 {
                                        x.fetch_sub(size.abs() as usize, SeqCst);
                                    } else {
                                        x.fetch_add(size as usize, SeqCst);
                                    }
                                });
                                self.cur_count.fetch_sub(1, SeqCst);
                                behind
                            }
                            Evict::None => behind.get_next(),
                        };

                        unsafe { next.load(SeqCst).as_ref().unwrap() }
                    }
                    Access::H { .. } => break Ok(()),
                    _ => unreachable!(),
                }
            },
            None => Ok(()),
        }
    }
}
