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

use crate::clru::{self, Access, Evictor};

pub struct Lru<K, V, H = cmap::DefaultHasher>
where
    K: Clone + PartialEq + Hash,
    V: Clone,
    H: BuildHasher,
{
    max_count: usize,
    max_old: Duration,
    hash_builder: H,

    maps: Vec<cmap::Map<K, clru::Value<K, V>, H>>,
    access_lists: Vec<Arc<Access<K>>>,

    close: Arc<AtomicBool>,
}

impl<K, V, H> Drop for Lru<K, V, H>
where
    K: Clone + PartialEq + Hash,
    V: Clone,
    H: BuildHasher,
{
    fn drop(&mut self) {
        self.close.store(true, SeqCst);
    }
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
            hash_builder: self.hash_builder.clone(),

            maps: self.maps.iter().map(|m| m.clone()).collect(),
            access_lists: self.access_lists.iter().map(|a| Arc::clone(a)).collect(),

            close: Arc::clone(&self.close),
        }
    }
}

impl<K, V> Lru<K, V, cmap::DefaultHasher>
where
    K: 'static + Send + Sync + Clone + PartialEq + Hash,
    V: 'static + Send + Sync + Clone,
{
    pub fn new(
        max_count: usize,
        max_old: Duration,
        shards: usize,
        concurrency: usize,
    ) -> Self {
        Self::with_hash(
            max_count,
            max_old,
            shards,
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
        shards: usize,
        concurrency: usize,
        hash_builder: H,
    ) -> Lru<K, V, H> {
        let maps: Vec<cmap::Map<K, clru::Value<K, V>, H>> = {
            let concurrency = concurrency + 1;
            let iter =
                (0..shards).map(|_| cmap::Map::new(concurrency, hash_builder.clone()));
            iter.collect()
        };

        let close = Arc::new(AtomicBool::new(false));

        let val = Lru {
            max_count,
            max_old,
            hash_builder,

            maps,
            access_lists: (0..shards).map(|_| Access::new_list()).collect(),
            close,
        };

        for (i, map) in val.maps.iter().enumerate() {
            let map = map.clone();
            let close = Arc::clone(&val.close);
            let access_list = Arc::clone(&val.access_lists[i]);
            let mut evtor = Evictor::new(max_count, map, close, access_list);
            evtor.set_max_old(max_old);
            thread::spawn(move || evtor.run());
        }

        val
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + PartialEq + ?Sized + Hash,
    {
        let shard = {
            let hasher = self.hash_builder.build_hasher();
            (key_to_hash32(key, hasher) % (self.maps.len() as u32)) as usize
        };

        loop {
            let (map, access_list) = (&self.maps[shard], &self.access_lists[shard]);
            let new_ptr = Box::leak(Access::new(key.to_owned())) as *const Access<K>;

            let res = map.get_with(key, |cval: &clru::Value<K, V>| {
                let old = cval.access.load(SeqCst);
                let new = new_ptr as *mut Access<K>;
                match cval.access.compare_exchange(old, new, SeqCst, SeqCst) {
                    Ok(_) => {
                        unsafe { old.as_ref().unwrap() }.delete();
                        access_list.prepend(unsafe { Box::from_raw(new) });
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

    pub fn set(&mut self, key: K, value: V) {
        let shard = {
            let hasher = self.hash_builder.build_hasher();
            (key_to_hash32(&key, hasher) % (self.maps.len() as u32)) as usize
        };

        let (map, access_list) = (&mut self.maps[shard], &self.access_lists[shard]);
        let access_ptr = Box::leak(Access::new(key.to_owned()));

        let value = clru::Value {
            value: Arc::new(value),
            access: AtomicPtr::new(access_ptr),
        };

        access_list.prepend(unsafe { Box::from_raw(access_ptr) });
        match map.set(key, value) {
            Some(clru::Value { access, .. }) => {
                let access = unsafe { access.load(SeqCst).as_ref().unwrap() };
                access.delete()
            }
            None => (),
        }
    }

    pub fn remove<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: ?Sized + PartialEq + Hash,
    {
        let shard = {
            let hasher = self.hash_builder.build_hasher();
            (key_to_hash32(&key, hasher) % (self.maps.len() as u32)) as usize
        };

        let map = &mut self.maps[shard];

        match map.remove(key) {
            Some(clru::Value { access, .. }) => {
                let access = unsafe { access.load(SeqCst).as_ref().unwrap() };
                access.delete()
            }
            None => (),
        }
    }
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
