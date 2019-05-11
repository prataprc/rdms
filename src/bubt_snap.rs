// TODO: review resize() calls
// TODO: review "as type conversions" for llrb-index jsondata

use std::{marker, path};

use crate::bubt_config::{Config, MetaItem};
use crate::bubt_stats::Stats;
use crate::core::{Diff, Entry, Result, Serialize};
use crate::error::BognError;

pub struct Snapshot<K, V>
where
    K: Ord + Clone + Serialize,
    V: Default + Clone + Diff + Serialize,
{
    name: String,
    config: Config,
    metadata: Vec<u8>,
    stats: Stats,
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
            name: name.to_string(),
            config: Default::default(),
            metadata: Default::default(),
            stats: Default::default(),
            root: Default::default(),

            phantom_key: marker::PhantomData,
            phantom_val: marker::PhantomData,
        };

        let mut iter = Config::open_index(dir, name)?.into_iter();
        if let Some(MetaItem::Marker(_)) = iter.next() {
            if let Some(MetaItem::Metadata(_)) = iter.next() {
                // DO nothing
            } else {
                return Err(BognError::InvalidSnapshot("expected metadata".to_string()));
            }

            if let Some(MetaItem::Stats(stats)) = iter.next() {
                snap.stats = stats.parse()?;
                let mut config: Config = snap.stats.clone().into();
                config.dir = dir.to_string();
                config.vlog_file = match config.vlog_file.clone() {
                    None => None,
                    Some(vlog_file) => {
                        let mut file = path::PathBuf::new();
                        let ifile = Config::index_file(&dir, &name);
                        file.push(path::Path::new(&ifile).parent().unwrap());
                        file.push(path::Path::new(&vlog_file).file_name().unwrap());
                        Some(file.to_str().unwrap().to_string())
                    }
                };
            } else {
                return Err(BognError::InvalidSnapshot(
                    "expected statistics".to_string(),
                ));
            }

            if let Some(MetaItem::Root(root)) = iter.next() {
                snap.root = root;
            } else {
                return Err(BognError::InvalidSnapshot("expected root".to_string()));
            }

            if iter.next().is_some() {
                return Err(BognError::InvalidSnapshot(
                    "unexpected meta item".to_string(),
                ));
            }
        } else {
            return Err(BognError::InvalidSnapshot("expected marker".to_string()));
        }
        Ok(snap)
    }
}
