use std::{
    fmt,
    hash::{BuildHasher, Hash},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    thread, time,
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
pub struct Evictor<K, V, H>
where
    K: fmt::Debug,
{
    max_size: Option<usize>,
    cur_size: Option<Arc<AtomicUsize>>,
    max_count: usize,
    cur_count: Arc<AtomicUsize>,
    max_old: Option<time::Duration>,

    map: cmap::Map<K, Arc<clru::Value<K, V>>, H>,
    access_tail: Arc<Access<K>>,
    close: Arc<AtomicBool>,

    pub(crate) n_evicted: usize,
    pub(crate) n_deleted: usize,
    pub(crate) n_gc: usize,
    pub(crate) n_access_gc: usize,
}

#[derive(Debug)]
enum Evict {
    Deleted,
    Ok,
    None,
}

impl<K, V, H> Evictor<K, V, H>
where
    K: fmt::Debug,
{
    /// Create a new evictor.
    pub fn new(
        config: &Config,
        close: Arc<AtomicBool>,
        access_tail: Arc<Access<K>>,
        map: cmap::Map<K, Arc<clru::Value<K, V>>, H>,
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
            n_gc: 0,
            n_access_gc: 0,
        }
    }
}

impl<K, V, H> Evictor<K, V, H>
where
    K: Clone + PartialEq + Hash + fmt::Debug,
    V: Clone + dbs::Footprint,
    H: BuildHasher,
{
    pub fn run(mut self) -> Result<Self> {
        let mut garbage_access = vec![];
        let mut seqno = u64::MAX;
        seqno = loop {
            if self.close.load(SeqCst) {
                break seqno;
            }
            {
                let (a, b) = self.do_eviction(garbage_access)?;
                garbage_access = a;
                seqno = b;
            }
            // println!("eviction loop n:{}", self.n_gc);
            thread::yield_now();
            garbage_access = self.gc(garbage_access, self.map.gc_epoch(seqno), false)
        };

        for access in self.do_eviction_all().into_iter() {
            garbage_access.push(access);
            self.n_access_gc += 1;
        }

        // actually free all evictions and deletions.
        while garbage_access.len() > 0 {
            garbage_access = self.gc(garbage_access, self.map.gc_epoch(seqno), true)
        }

        Ok(self)
    }

    fn gc(
        &mut self,
        garbage_access: Vec<Box<Access<K>>>,
        gc_epoch: u64,
        force: bool,
    ) -> Vec<Box<Access<K>>> {
        self.n_gc = self.n_gc.saturating_add(1);

        //println!(
        //    "eviction gc-epoch:{} len:{}, epochs:{:?}",
        //    gc_epoch,
        //    garbage_access.len(),
        //    garbage_access
        //        .iter()
        //        .map(|x| (
        //            x.as_key(),
        //            x.to_epoch(),
        //            format!("{:p}", x.as_ref()),
        //            x.is_deleted(),
        //        ))
        //        .collect::<Vec<(&K, u64, String, bool)>>()
        //);

        let mut rems = vec![];
        for access in garbage_access.into_iter() {
            match access.as_ref() {
                Access::N { epoch, .. } if force || (*epoch < gc_epoch) => {
                    // drop access here, collected as garbage.
                    ()
                }
                Access::N { .. } => rems.push(access),
                _ => unreachable!(),
            }
        }
        rems
    }

    fn to_evict(&self, node: &Access<K>) -> Evict {
        let res = match node {
            Access::N { deleted, born, .. } => match deleted.load(SeqCst) {
                true => Evict::Deleted,
                false => {
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
            },
            _ => unreachable!(),
        };

        //{
        //    let cur_count = self.cur_count.load(SeqCst);
        //    println!("to_evict {:?} cur_count:{}", res, cur_count);
        //}

        res
    }

    fn do_eviction(
        &mut self,
        mut garbage_access: Vec<Box<Access<K>>>,
    ) -> Result<(Vec<Box<Access<K>>>, u64)> {
        let seqno = self.map.epoch();
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

        // println!("do_eviction behind {:p}", behind);
        match ahead {
            Some(mut ahead) => loop {
                ahead = match ahead {
                    Access::N { next, .. } => {
                        behind = match self.to_evict(behind.get_next()) {
                            Evict::Deleted => {
                                self.n_deleted += 1;
                                let mut next_access = behind.delete_next();
                                next_access.set_epoch(self.map.epoch());
                                //println!(
                                //    "evict:deleted, access:{:p} ahead:{:p}",
                                //    next_access.as_ref(),
                                //    ahead
                                //);

                                garbage_access.push(next_access);
                                behind
                            }
                            Evict::Ok => {
                                self.n_evicted += 1;

                                // IMPORTANT: delete_next() and map.remove() have
                                // synchronisation problem. Sequence is important.
                                let key = behind.next_key();
                                let (remok, size) = match self.map.remove(key) {
                                    Some(value) => (1, value.footprint()?),
                                    None => (0, 0),
                                };

                                let mut next_access = behind.delete_next();
                                next_access.set_epoch(self.map.epoch());
                                //println!(
                                //    "evict remove key:{:?} access:{:p}",
                                //    key,
                                //    next_access.as_ref()
                                //);
                                garbage_access.push(next_access);

                                self.cur_size.as_ref().map(|x| {
                                    if size < 0 {
                                        x.fetch_sub(size.abs() as usize, SeqCst);
                                    } else {
                                        x.fetch_add(size as usize, SeqCst);
                                    }
                                });
                                self.cur_count.fetch_sub(remok, SeqCst);
                                //println!(
                                //    "evict:ok, behind_next:{:p} ahead:{:p}",
                                //    behind.get_next(),
                                //    ahead
                                //);
                                behind
                            }
                            Evict::None => {
                                //println!(
                                //    "evict:none, behind_next:{:p} ahead:{:p}",
                                //    behind.get_next(),
                                //    ahead
                                //);
                                behind.get_next()
                            }
                        };

                        unsafe { next.load(SeqCst).as_ref().unwrap() }
                    }
                    Access::H { .. } => break Ok(()),
                    _ => unreachable!(),
                }
            },
            None => Ok(()),
        }?;

        Ok((garbage_access, seqno))
    }

    fn do_eviction_all(&self) -> Vec<Box<Access<K>>> {
        let mut node: &Access<K> = self.access_tail.as_ref();
        let mut garbage_access = vec![];
        loop {
            node = match node {
                Access::N { next, .. } => {
                    let mut access = unsafe {
                        Box::from_raw(node as *const Access<K> as *mut Access<K>)
                    };
                    access.set_epoch(0);
                    garbage_access.push(access);
                    unsafe { next.load(SeqCst).as_ref().unwrap() }
                }
                Access::T { next } => unsafe { next.load(SeqCst).as_ref().unwrap() },
                Access::H { .. } => break,
            };
        }

        garbage_access
    }
}
