use std::{
    borrow::Borrow,
    fmt,
    hash::{BuildHasher, Hash},
    sync::{
        atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    thread, time,
};

use crate::{
    clru::{self, Access, Evictor},
    dbs, Result,
};

pub struct Config {
    pub thread_pool_size: usize,
    pub max_size: Option<usize>,
    pub max_count: usize,
    pub max_old: Option<u64>, // in seconds.
    pub(crate) cur_size: Option<Arc<AtomicUsize>>,
    pub(crate) cur_count: Arc<AtomicUsize>,
}

impl Config {
    pub fn new(thread_pool_size: usize, max_count: usize) -> Config {
        Config {
            thread_pool_size,
            max_size: None,
            max_count,
            max_old: None,
            cur_size: None,
            cur_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn set_max_size(&mut self, max_size: usize) -> &mut Self {
        self.max_size = Some(max_size);
        self.cur_size = Some(Arc::new(AtomicUsize::new(0)));
        self
    }

    pub fn set_max_old(&mut self, max_old: u64) -> &mut Self {
        self.max_old = Some(max_old);
        self
    }
}

pub struct Lru<K, V, H = cmap::DefaultHasher>
where
    K: fmt::Debug,
{
    max_size: Option<usize>,
    cur_size: Option<Arc<AtomicUsize>>,
    max_count: usize,
    cur_count: Arc<AtomicUsize>,
    max_old: Option<time::Duration>,

    map: cmap::Map<K, Arc<clru::Value<K, V>>, H>,
    access_head: Arc<Access<K>>,
    access_tail: Arc<Access<K>>,
    evictor: Option<thread::JoinHandle<Result<Evictor<K, V, H>>>>,
    close: Arc<AtomicBool>,

    n_gets: Arc<AtomicUsize>,
    n_sets: Arc<AtomicUsize>,
}

impl<K, V, H> Clone for Lru<K, V, H>
where
    K: fmt::Debug,
{
    fn clone(&self) -> Self {
        Lru {
            max_size: self.max_size,
            cur_size: self.cur_size.as_ref().map(Arc::clone),
            max_count: self.max_count,
            cur_count: Arc::clone(&self.cur_count),
            max_old: self.max_old,

            map: self.map.cloned(),
            access_head: Arc::clone(&self.access_head),
            access_tail: Arc::clone(&self.access_tail),
            evictor: None,
            close: Arc::clone(&self.close),

            n_gets: Arc::clone(&self.n_gets),
            n_sets: Arc::clone(&self.n_sets),
        }
    }
}

impl<K, V> Lru<K, V, cmap::DefaultHasher>
where
    K: 'static + Send + Sync + Clone + PartialEq + Hash + fmt::Debug,
    V: 'static + Send + Sync + Clone + dbs::Footprint,
{
    pub fn from_config(config: Config) -> Self {
        Lru::with_hash(cmap::DefaultHasher::default(), config)
    }
}

impl<K, V, H> Lru<K, V, H>
where
    K: 'static + Send + Sync + Clone + PartialEq + Hash + fmt::Debug,
    V: 'static + Send + Sync + Clone + dbs::Footprint,
    H: 'static + Send + Sync + Clone + BuildHasher,
{
    pub fn with_hash(hash_builder: H, mut config: Config) -> Lru<K, V, H> {
        config.cur_size = config.max_size.map(|_| Arc::new(AtomicUsize::new(0)));
        config.cur_count = Arc::new(AtomicUsize::new(0));

        let (access_head, access_tail) = Access::new_list();
        let close = Arc::new(AtomicBool::new(false));

        let map: cmap::Map<K, Arc<clru::Value<K, V>>, H> =
            { cmap::Map::new(config.thread_pool_size + 1, hash_builder) };

        let evictor = {
            let evictor = Evictor::new(
                &config,
                Arc::clone(&close),
                Arc::clone(&access_tail),
                map.cloned(),
            );
            Some(thread::spawn(move || evictor.run()))
        };

        Lru {
            max_size: config.max_size,
            cur_size: config.cur_size,
            max_count: config.max_count,
            cur_count: Arc::clone(&config.cur_count),
            max_old: config.max_old.map(time::Duration::from_secs),

            map,
            access_head,
            access_tail,
            evictor,
            close,

            n_gets: Arc::new(AtomicUsize::new(0)),
            n_sets: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn close(mut self) -> Result<Option<Stats>> {
        let _access_head = loop {
            if Arc::get_mut(&mut self.access_head).is_some() {
                break Arc::try_unwrap(self.access_head).ok().unwrap();
            }
        };

        let stats = match self.evictor.take() {
            Some(evictor) => {
                self.close.store(true, SeqCst);
                let evictor = match evictor.join() {
                    Ok(res) => res?,
                    Err(err) => std::panic::resume_unwind(err),
                };
                let stats = Stats {
                    n_gets: self.n_gets.load(SeqCst),
                    n_sets: self.n_sets.load(SeqCst),
                    n_evicted: evictor.n_evicted,
                    n_deleted: evictor.n_deleted,
                    n_gc: evictor.n_gc,
                    n_access_gc: evictor.n_access_gc,
                };

                Some(stats)
            }
            None => None,
        };

        let _access_tail = Arc::try_unwrap(self.access_tail).ok().unwrap();

        Ok(stats)
    }
}

impl<K, V, H> Lru<K, V, H>
where
    K: fmt::Debug,
{
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        V: Clone,
        Q: ToOwned<Owned = K> + PartialEq + ?Sized + Hash,
        H: BuildHasher,
    {
        self.n_gets.fetch_add(1, SeqCst);

        loop {
            let res = self.map.get_with(key, |cval: &Arc<clru::Value<K, V>>| {
                let new_ptr = Box::leak(self.access_head.new(key));
                let old = cval.access.load(SeqCst);
                match cval.access.compare_exchange(old, new_ptr, SeqCst, SeqCst) {
                    Ok(_) => {
                        // println!("lru.get::get_with old:{:p}", old);
                        unsafe { old.as_ref().unwrap() }.delete();
                        self.access_head.append(unsafe { Box::from_raw(new_ptr) });
                        AccessResult::Ok(cval.value.clone())
                    }
                    Err(_) => {
                        // println!("lru.get::get_with loop back {:p}", old);
                        let _drop_node = unsafe { Box::from_raw(new_ptr) };
                        AccessResult::Retry
                    }
                }
            });

            match res {
                Some(AccessResult::Ok(value)) => break Some(value),
                Some(AccessResult::Retry) => (),
                None => break None,
            }
            // println!("get looping back");
        }
    }

    pub fn set(&mut self, key: K, value: V) -> Option<V>
    where
        K: Clone + PartialEq + Hash,
        V: Clone,
        H: BuildHasher,
    {
        self.n_sets.fetch_add(1, SeqCst);

        let new_ptr = Box::leak(self.access_head.new(&key));

        let value = Arc::new(clru::Value { value, access: AtomicPtr::new(new_ptr) });

        let res = match self.map.set(key, value).as_ref().map(|x| x.as_ref()) {
            Some(clru::Value { access, value }) => {
                let access = unsafe { access.load(SeqCst).as_ref().unwrap() };
                access.delete();
                Some(value.clone())
            }
            None => {
                self.cur_count.fetch_add(1, SeqCst);
                None
            }
        };

        self.access_head.append(unsafe { Box::from_raw(new_ptr) });

        res
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }
}

enum AccessResult<V> {
    Ok(V),
    Retry,
}

#[derive(Debug)]
pub struct Stats {
    pub n_gets: usize,
    pub n_sets: usize,
    // evictor stats
    pub n_evicted: usize,
    pub n_deleted: usize,
    pub n_gc: usize,
    pub n_access_gc: usize,
}

#[cfg(test)]
#[path = "lru_test.rs"]
mod lru_test;
