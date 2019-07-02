// Takes care of, batching entries, serializing and appending them to disk,
// commiting the appended batch(es).

use std::sync::atomic::AtomicU64;
use std::{collections::HashMap, ffi, fs};

use crate::core::Serialize;
use crate::error::Error;
use crate::wal_thread::{Journal, Shard};

pub struct Wal<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    name: String,
    dir: ffi::OsString,
    index: AtomicU64,
    shards: Vec<Shard<K, V>>, // shard-id start from `1`
}

impl<K, V> Wal<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    pub fn create(
        name: String,
        dir: ffi::OsString,
        nshards: usize, // number of shards
    ) -> Result<Wal<K, V>, Error> {
        // purge existing journals for name.
        for item in fs::read_dir(&dir)? {
            let file_name = item?.file_name();
            match Journal::<K, V>::load(name.clone(), file_name)? {
                Some(ref mut jrn) => jrn.purge()?,
                None => (),
            }
        }
        // create shards
        let mut shards = vec![];
        for id in 1..(nshards + 1) {
            let mut shard = Shard::<K, V>::new(name.clone(), id);
            shard.add_journal(Journal::create(name.clone(), id)?);
            shards.push(shard);
        }
        Ok(Wal {
            name,
            dir,
            index: AtomicU64::new(0),
            shards: vec![],
        })
    }

    pub fn load(name: String, dir: ffi::OsString) -> Result<Wal<K, V>, Error> {
        let mut shards: HashMap<usize, Shard<K, V>> = HashMap::new();
        for item in fs::read_dir(&dir)? {
            let dentry = item?;
            // can this be a journal file ?
            if let Some(jrn) = Journal::load(name.clone(), dentry.file_name())? {
                let id = jrn.id();
                match shards.get_mut(&id) {
                    Some(shard) => shard.add_journal(jrn),
                    None => {
                        let mut shard = Shard::new(name.clone(), id);
                        shard.add_journal(jrn);
                        shards.insert(id, shard);
                    }
                }
            }
        }
        let mut shards: Vec<Shard<K, V>> = /* transform map to vector */
            shards.into_iter().map(|(_, v)| v).collect();
        shards.sort_by_key(|shard| shard.id());
        for (i, shard) in shards.iter().enumerate() {
            if i != shard.id() - 1 {
                let msg = format!("invalid shard at {}", i);
                return Err(Error::InvalidWAL(msg));
            }
        }
        Ok(Wal {
            name,
            dir,
            index: AtomicU64::new(0),
            shards,
        })
    }
}
