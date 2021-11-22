use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
    sync::{
        atomic::{AtomicBool, AtomicPtr, Ordering::SeqCst},
        Arc,
    },
    thread,
    time::Duration,
};

use crate::{
    clru::{self, Access, Evictor},
    Result,
};

pub struct Lru<K, V, H = cmap::DefaultHasher>
where
    K: Clone + PartialEq + Hash,
    V: Clone,
    H: BuildHasher,
{
    max_count: usize,
    max_old: Duration,

    map: cmap::Map<K, clru::Value<K, V>, H>,
    access_list: Arc<Access<K>>,
    evictor: Arc<thread::JoinHandle<Result<Evictor<K, V, H>>>>,
    // writer: Arc<thread::JoinHandle<Result<Writer>>>,
    close: Arc<AtomicBool>,
}

impl<K, V, H> Clone for Lru<K, V, H>
where
    K: Clone + PartialEq + Hash,
    V: Clone,
    H: Clone + BuildHasher,
{
    fn clone(&self) -> Self {
        Lru {
            max_count: self.max_count,
            max_old: self.max_old,

            map: self.map.clone(),
            access_list: Arc::clone(&self.access_list),
            evictor: Arc::clone(&self.evictor),
            // writer: Arc<thread::JoinHandle<Result<Writer>>>,
            close: Arc::clone(&self.close),
        }
    }
}

impl<K, V> Lru<K, V, cmap::DefaultHasher>
where
    K: 'static + Send + Sync + Clone + PartialEq + Hash,
    V: 'static + Send + Sync + Clone,
{
    pub fn new(max_count: usize, max_old: Duration, concurrency: usize) -> Self {
        Self::with_hash(
            max_count,
            max_old,
            concurrency,
            cmap::DefaultHasher::default(),
        )
    }
}

impl<K, V, H> Lru<K, V, H>
where
    K: 'static + Send + Sync + Clone + PartialEq + Hash,
    V: 'static + Send + Sync + Clone,
    H: 'static + Send + Sync + Clone + BuildHasher,
{
    pub fn with_hash(
        max_count: usize,
        max_old: Duration,
        concurrency: usize,
        hash_builder: H,
    ) -> Lru<K, V, H> {
        let map: cmap::Map<K, clru::Value<K, V>, H> =
            { cmap::Map::new(concurrency + 1, hash_builder) };
        let access_list = Access::new_list();
        let close = Arc::new(AtomicBool::new(false));

        let evictor = {
            let mut evictor = Evictor::new(
                max_count,
                map.clone(),
                Arc::clone(&close),
                Arc::clone(&access_list),
            );
            evictor.set_max_old(max_old);
            Arc::new(thread::spawn(move || evictor.run()))
        };

        let val = Lru {
            max_count,
            max_old,

            map,
            access_list,
            evictor,

            close,
        };

        val
    }

    fn close(self) -> Result<()> {
        self.close.store(true, SeqCst);
        match Arc::try_unwrap(self.evictor) {
            Ok(evictor) => {
                evictor.join().ok(); // TODO: update Stats
            }
            Err(_evictor) => (), // drop reference count
        }

        Ok(())
    }
}

impl<K, V, H> Lru<K, V, H>
where
    K: 'static + Send + Sync + Clone + PartialEq + Hash,
    V: 'static + Send + Sync + Clone,
    H: 'static + Send + Sync + Clone + BuildHasher,
{
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + PartialEq + ?Sized + Hash,
    {
        loop {
            let new_ptr = Box::leak(Access::new(key.to_owned())) as *const Access<K>;

            let res = self.map.get_with(key, |cval: &clru::Value<K, V>| {
                let old = cval.access.load(SeqCst);
                let new = new_ptr as *mut Access<K>;
                match cval.access.compare_exchange(old, new, SeqCst, SeqCst) {
                    Ok(_) => {
                        unsafe { old.as_ref().unwrap() }.delete();
                        self.access_list.prepend(unsafe { Box::from_raw(new) });
                        AccessResult::Ok(cval.value.as_ref().clone())
                    }
                    Err(_) => {
                        let _access = unsafe { Box::from_raw(new) }; // drop this access
                        AccessResult::Retry
                    }
                }
            });

            match res {
                Some(AccessResult::Ok(value)) => break Some(value),
                Some(AccessResult::Retry) => (),
                None => break None,
            }
        }
    }

    //pub fn set(&mut self, key: K, value: V) {
    //    let (map, access_list) = (&mut self.maps[shard], &self.access_lists[shard]);
    //    let new_ptr = Box::leak(Access::new(key.to_owned()));

    //    let value = clru::Value {
    //        value: Arc::new(value),
    //        access: AtomicPtr::new(new_ptr),
    //    };

    //    access_list.prepend(unsafe { Box::from_raw(new_ptr) });
    //    match map.set(key, value) {
    //        Some(clru::Value { access, .. }) => {
    //            let access = unsafe { access.load(SeqCst).as_ref().unwrap() };
    //            access.delete()
    //        }
    //        None => (),
    //    }
    //}

    //pub fn remove<Q>(&mut self, key: &Q)
    //where
    //    K: Borrow<Q>,
    //    Q: ?Sized + PartialEq + Hash,
    //{
    //    let map = &mut self.maps[shard];

    //    match map.remove(key) {
    //        Some(clru::Value { access, .. }) => {
    //            let access = unsafe { access.load(SeqCst).as_ref().unwrap() };
    //            access.delete()
    //        }
    //        None => (),
    //    }
    //}
}

fn key_to_hash32<K, H>(key: &K, mut hasher: H) -> u32
where
    K: Hash + ?Sized,
    H: Hasher,
{
    key.hash(&mut hasher);
    let code: u64 = hasher.finish();
    (((code >> 32) ^ code) & 0xFFFFFFFF) as u32
}

enum AccessResult<V> {
    Ok(V),
    Retry,
}
