use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        mpsc, Arc,
    },
    time,
};

use crate::{clru::Access, Result};

/// Note that evictor will remove an access node if,
///
/// * Node is marked as deleted.
/// * Node is older than configured elapsed time, optional.
/// * Number of nodes in the access list exceed the count-limit, optional.
/// * Cummulative size of values held in cache exceeds size-limit, optional.
pub struct Evictor<K> {
    max_size: Option<usize>,
    cur_size: Option<Arc<AtomicUsize>>,
    max_count: Option<usize>,
    cur_count: Option<Arc<AtomicUsize>>,
    max_old: Option<time::Duration>,

    close: Arc<AtomicBool>,
    access_tail: Arc<Access<K>>,
    tx_writer: mpsc::SyncSender<()>,

    n_evicted: usize,
    n_deleted: usize,
}

enum Evict {
    Deleted,
    Ok,
    None,
}

impl<K> Evictor<K> {
    /// Create a new evictor.
    pub fn new(
        close: Arc<AtomicBool>,
        access_tail: Arc<Access<K>>,
        tx_writer: mpsc::SyncSender<()>,
    ) -> Self {
        Evictor {
            max_size: None,
            cur_size: None,
            max_count: None,
            cur_count: None,
            max_old: None,

            close,
            access_tail,
            tx_writer,

            n_evicted: 0,
            n_deleted: 0,
        }
    }

    pub fn set_max_size(
        &mut self,
        max_size: usize,
        cur_size: Arc<AtomicUsize>,
    ) -> &mut Self {
        self.max_size = Some(max_size);
        self.cur_size = Some(cur_size);
        self
    }

    pub fn set_max_count(
        &mut self,
        max_count: usize,
        cur_count: Arc<AtomicUsize>,
    ) -> &mut Self {
        self.max_count = Some(max_count);
        self.cur_count = Some(cur_count);
        self
    }

    pub fn set_max_old(&mut self, max_old: time::Duration) -> &mut Self {
        self.max_old = Some(max_old);
        self
    }
}

impl<K> Evictor<K> {
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
            Access::N {
                deleted,
                born,
                next,
                ..
            } => {
                match deleted.load(SeqCst) {
                    true => return Evict::Deleted,
                    false => (),
                }

                let cur_size = self.cur_size.as_ref().map(|x| x.load(SeqCst));
                let mut evicta = match cur_size {
                    Some(cur_size) if cur_size > self.max_size.unwrap() => true,
                    Some(_) | None => false,
                };

                let cur_count = self.cur_count.as_ref().map(|x| x.load(SeqCst));
                evicta = evicta
                    || match cur_count {
                        Some(cur_count) if cur_count > self.max_count.unwrap() => true,
                        Some(_) | None => false,
                    };

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
                                // TODO: key to be removed from the cache.
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
