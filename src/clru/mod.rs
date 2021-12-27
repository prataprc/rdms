//! Module implement concurrent LRU cache.

use std::{fmt, sync::atomic::AtomicPtr};

use crate::{dbs, Result};

mod access;
mod evictor;
mod lru;

use access::Access;
use evictor::Evictor;
pub use lru::{Config, Lru, Stats};

// wrap the value parameter.
pub struct Value<K, V>
where
    K: fmt::Debug,
{
    value: V,
    access: AtomicPtr<Access<K>>,
}

impl<K, V> dbs::Footprint for Value<K, V>
where
    K: fmt::Debug,
    V: dbs::Footprint,
{
    fn footprint(&self) -> Result<isize> {
        let mut size = std::mem::size_of_val(self) as isize;
        size += self.value.footprint()?;
        Ok(size)
    }
}
