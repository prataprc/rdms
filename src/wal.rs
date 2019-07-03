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
    nshards: (usize, usize), // (configured, active)
    journals: Vec<Journal<K, V>>,
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
                Some(jrn) => jrn.purge()?,
                None => (),
            }
        }
        // create this WAL. later shards/journals can be added.
        Ok(Wal {
            name,
            dir,
            index: AtomicU64::new(0),
            nshards: (nshards, 0),
            journals: vec![],
        })
    }

    pub fn load(name: String, dir: ffi::OsString) -> Result<Wal<K, V>, Error> {
        let mut shards: HashMap<usize, bool> = HashMap::new();
        let mut journals = vec![];
        for item in fs::read_dir(&dir)? {
            let dentry = item?;
            // can this be a journal file ?
            if let Some(jrn) = Journal::load(name.clone(), dentry.file_name())? {
                match shards.get_mut(&jrn.id()) {
                    Some(_) => (),
                    None => {
                        shards.insert(jrn.id(), true);
                    }
                }
                journals.push(jrn);
            }
        }
        let mut shards: Vec<usize> = shards.into_iter().map(|(k, _)| k).collect();
        shards.sort();
        for (i, id) in shards.iter().enumerate() {
            if i != id - 1 {
                let msg = format!("invalid shard at {}", i);
                return Err(Error::InvalidWAL(msg));
            }
        }

        Ok(Wal {
            name,
            dir,
            index: AtomicU64::new(0),
            nshards: (shards.len(), 0),
            journals,
        })
    }

    pub fn new_shard(&mut self) -> Result<Shard<K, V>, Error> {
        if self.nshards.1 < self.nshards.0 {
            let id = self.nshards.1 + 1;
            let mut shard = Shard::<K, V>::new(self.name.clone(), id);

            // remove journals for this shard.
            let journals: Vec<Journal<K, V>> =
                self.journals.drain_filter(|jrn| jrn.id() == id).collect();
            journals.into_iter().for_each(|jrn| shard.add_journal(jrn));

            self.nshards.1 += 1;
            Ok(shard)
        } else {
            Err(Error::InvalidWAL(format!("exceeding the shard limit")))
        }
    }

    //pub fn replay(self, ) -> impl Iterator<Item = Entry<K, V>> {
    //}

    pub fn purge(self) -> Result<(), Error> {
        for jrn in self.journals.into_iter() {
            jrn.purge()?;
        }
        Ok(())
    }
}
