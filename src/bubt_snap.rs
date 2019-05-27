// TODO: review resize() calls
// TODO: review "as type conversions" for llrb-index jsondata
// TODO: implement log() and validate() API.

use std::{fs, marker, path};

use crate::bubt_config::{self, Config, MetaItem};
use crate::bubt_stats::Stats;
use crate::core::{Diff, Result, Serialize};
use crate::error::BognError;

pub struct Snapshot<K, V>
where
    K: Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    config: Config,
    stats: Stats,
    metadata: Vec<u8>,
    root: u64,

    phantom_key: marker::PhantomData<K>,
    phantom_val: marker::PhantomData<V>,
}

impl<K, V> Snapshot<K, V>
where
    K: Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    pub fn open(dir: &str, name: &str) -> Result<Snapshot<K, V>> {
        let mut snap = Snapshot {
            metadata: Default::default(),
            stats: Default::default(),
            config: Default::default(),
            root: Default::default(),

            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };

        let mut iter = bubt_config::read_meta_items(dir, name)?.into_iter();
        // read and discard marker
        match iter.next() {
            Some(MetaItem::Marker(_)) => (),
            Some(item) => {
                let err = format!("expected marker, found {}", item);
                return Err(BognError::InvalidSnapshot(err));
            }
            None => {
                let err = "expected marker, eof".to_string();
                return Err(BognError::InvalidSnapshot(err));
            }
        }
        // read metadata
        snap.metadata = match iter.next() {
            Some(MetaItem::Metadata(metadata)) => metadata,
            Some(item) => {
                let err = format!("expected metadata, found {}", item);
                return Err(BognError::InvalidSnapshot(err));
            }
            None => {
                let err = "expected metadata, eof".to_string();
                return Err(BognError::InvalidSnapshot(err));
            }
        };
        // read the statistics and information for this snapshot.
        snap.stats = match iter.next() {
            Some(MetaItem::Stats(stats)) => stats.parse()?,
            Some(item) => {
                let err = format!("expected metadata, found {}", item);
                return Err(BognError::InvalidSnapshot(err));
            }
            None => {
                let err = "expected statistics".to_string();
                return Err(BognError::InvalidSnapshot(err));
            }
        };
        snap.config = snap.stats.clone().into();
        snap.config.dir = dir.to_string();
        snap.config.vlog_file = match snap.config.vlog_file.clone() {
            None => None,
            Some(vlog_file_1) => {
                let f = path::Path::new(&vlog_file_1).file_name().unwrap();
                let ifile = Config::index_file(&dir, &name);
                let mut file = path::PathBuf::new();
                file.push(path::Path::new(&ifile).parent().unwrap());
                file.push(f);
                let vlog_file_2 = file.to_str().unwrap().to_string();
                // TODO: verify whether both the file names are equal.
                Some(vlog_file_2)
            }
        };
        // read root
        snap.root = match iter.next() {
            Some(MetaItem::Root(root)) => root,
            Some(item) => {
                let err = format!("expected metadata, found {}", item);
                return Err(BognError::InvalidSnapshot(err));
            }
            None => {
                let err = "expected statistics".to_string();
                return Err(BognError::InvalidSnapshot(err));
            }
        };
        // make sure nothing is left !!
        if let Some(item) = iter.next() {
            let err = format!("expected eof, found {}", item);
            return Err(BognError::InvalidSnapshot(err));
        }
        // validate snapshot
        if snap.stats.name != name {
            let err = format!("name mistmatch {} != {}", snap.stats.name, name);
            return Err(BognError::InvalidSnapshot(err));
        }
        // Okey dockey
        Ok(snap)
    }

    pub fn purge(&mut self) {
        panic!("to-be-implemented")
    }

    pub fn count(&self) -> u64 {
        self.stats.n_count
    }

    pub fn footprint(&self) -> u64 {
        let index_file = Config::index_file(&self.config.dir, &self.config.name);
        let mut footprint = fs::metadata(index_file).unwrap().len();

        let vlog_file = Config::vlog_file(&self.config.dir, &self.config.name);
        footprint += fs::metadata(vlog_file).unwrap().len();
        footprint
    }

    pub fn get_seqno(&self) -> u64 {
        self.stats.seqno
    }

    pub fn metadata(&self) -> Vec<u8> {
        self.metadata.clone()
    }

    pub fn stats(&self) -> Stats {
        self.stats.clone()
    }

    pub fn close(self) {
        // TODO: can be implemented via Drop.
        panic!("to-be-implemented")
    }

    // get
    // iter
    // range
}
