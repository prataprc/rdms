use std::{
    borrow::Borrow,
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
    pub pool_size: usize,
    pub write_buffer: usize,
    pub max_size: Option<usize>,
    pub max_count: usize,
    pub max_old: Option<u64>, // in seconds.
    pub(crate) cur_size: Option<Arc<AtomicUsize>>,
    pub(crate) cur_count: Arc<AtomicUsize>,
}

pub struct Lru<K, V, H = cmap::DefaultHasher> {
    max_size: Option<usize>,
    cur_size: Option<Arc<AtomicUsize>>,
    max_count: usize,
    cur_count: Arc<AtomicUsize>,
    max_old: Option<time::Duration>,

    map: cmap::Map<K, clru::Value<K, V>, H>,
    access_head: Arc<Access<K>>,
    access_tail: Arc<Access<K>>,
    evictor: Option<thread::JoinHandle<Result<Evictor<K, V, H>>>>,
    close: Arc<AtomicBool>,

    n_access: Arc<AtomicUsize>,
}

impl<K, V, H> Clone for Lru<K, V, H> {
    fn clone(&self) -> Self {
        Lru {
            max_size: self.max_size.clone(),
            cur_size: self.cur_size.as_ref().map(Arc::clone),
            max_count: self.max_count.clone(),
            cur_count: Arc::clone(&self.cur_count),
            max_old: self.max_old.clone(),

            map: self.map.clone(),
            access_head: Arc::clone(&self.access_head),
            access_tail: Arc::clone(&self.access_tail),
            evictor: None,
            close: Arc::clone(&self.close),

            n_access: Arc::clone(&self.n_access),
        }
    }
}

impl<K, V> Lru<K, V, cmap::DefaultHasher>
where
    K: 'static + Send + Sync + Clone + PartialEq + Hash,
    V: 'static + Send + Sync + Clone + dbs::Footprint,
{
    pub fn from_config(config: Config) -> Self {
        Lru::with_hash(cmap::DefaultHasher::default(), config)
    }
}

impl<K, V, H> Lru<K, V, H>
where
    K: 'static + Send + Sync + Clone + PartialEq + Hash,
    V: 'static + Send + Sync + Clone + dbs::Footprint,
    H: 'static + Send + Sync + Clone + BuildHasher,
{
    pub fn with_hash(hash_builder: H, mut config: Config) -> Lru<K, V, H> {
        config.cur_size = match config.max_size.clone() {
            Some(_) => Some(Arc::new(AtomicUsize::new(0))),
            None => None,
        };
        config.cur_count = Arc::new(AtomicUsize::new(0));

        let (access_head, access_tail) = Access::new_list();
        let close = Arc::new(AtomicBool::new(false));

        let map: cmap::Map<K, clru::Value<K, V>, H> =
            { cmap::Map::new(config.pool_size + 1, hash_builder) };

        let evictor = {
            let evictor = Evictor::new(
                &config,
                Arc::clone(&close),
                Arc::clone(&access_tail),
                map.clone(),
            );
            Some(thread::spawn(move || evictor.run()))
        };

        Lru {
            max_size: config.max_size.clone(),
            cur_size: config.cur_size.clone(),
            max_count: config.max_count,
            cur_count: Arc::clone(&config.cur_count),
            max_old: config.max_old.map(time::Duration::from_secs),

            map,
            access_head,
            access_tail,
            evictor,
            close,

            n_access: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn close(mut self) -> Result<Option<Stats>> {
        match self.evictor.take() {
            Some(evictor) => {
                self.close.store(true, SeqCst);
                let evictor = match evictor.join() {
                    Ok(res) => res?,
                    Err(err) => std::panic::resume_unwind(err),
                };
                let stats = Stats {
                    n_evicted: evictor.n_evicted,
                    n_deleted: evictor.n_deleted,
                    n_access: self.n_access.load(SeqCst),
                };

                Ok(Some(stats))
            }
            None => Ok(None),
        }
    }
}

impl<K, V, H> Lru<K, V, H> {
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        V: Clone,
        Q: ToOwned<Owned = K> + PartialEq + ?Sized + Hash,
        H: BuildHasher,
    {
        let new_ptr = self.access_head.append(key);
        self.n_access.fetch_add(1, SeqCst);

        loop {
            let res = self.map.get_with(key, |cval: &clru::Value<K, V>| {
                let old = cval.access.load(SeqCst);
                match cval.access.compare_exchange(old, new_ptr, SeqCst, SeqCst) {
                    Ok(_) => {
                        unsafe { old.as_ref().unwrap() }.delete();
                        AccessResult::Ok(cval.value.as_ref().clone())
                    }
                    Err(_) => AccessResult::Retry,
                }
            });

            match res {
                Some(AccessResult::Ok(value)) => break Some(value.clone()),
                Some(AccessResult::Retry) => (),
                None => break None,
            }
        }
    }

    pub fn set(&mut self, key: K, value: V) -> Option<V>
    where
        K: Clone + PartialEq + Hash,
        V: Clone,
        H: BuildHasher,
    {
        let new_ptr = self.access_head.append(&key);
        self.n_access.fetch_add(1, SeqCst);

        let value = clru::Value {
            value: Arc::new(value),
            access: AtomicPtr::new(new_ptr),
        };
        match &self.map.set(key, value) {
            Some(clru::Value { access, value }) => {
                let access = unsafe { access.load(SeqCst).as_ref().unwrap() };
                access.delete();
                Some(value.as_ref().clone())
            }
            None => None,
        }
    }
}

enum AccessResult<V> {
    Ok(V),
    Retry,
}

pub struct Stats {
    pub n_evicted: usize,
    pub n_deleted: usize,
    pub n_access: usize,
}
