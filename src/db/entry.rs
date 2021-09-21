use cbordata::Cborize;

use std::{borrow::Borrow, fmt, ops::Bound, result};

use crate::{
    db::{Cutoff, Delta, Diff, Footprint, Value},
    Error, Result,
};

const ENTRY_VER: u32 = 0x00050001;

// TODO: test case for Cborize

// TODO: deriving Cborize on Entry has a problem, `deltas` field is using an
// associated type of Diff trait which is declared as constraint no `V`. Now
// automatically detecting this and deriving FromCbor and IntoCbor for
// <V as Diff>::Delta seem to be difficult. Hence we add the type parameter `D`
// to Entry. Is there an alternative ?

/// Entry type, describe a single `{key,value}` entry within indexed data-set.
#[derive(Clone, Cborize)]
pub struct Entry<K, V, D>
where
    V: Diff<Delta = D>,
{
    pub key: K,
    pub value: Value<V>,
    pub deltas: Vec<Delta<<V as Diff>::Delta>>, // from oldest to newest
}

impl<K, V, D> fmt::Debug for Entry<K, V, D>
where
    K: fmt::Debug,
    V: fmt::Debug + Diff<Delta = D>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}-{:?}", self.key, self.value)
    }
}

impl<K, V, D> PartialEq for Entry<K, V, D>
where
    K: PartialEq,
    V: PartialEq + Diff<Delta = D>,
    D: PartialEq,
{
    fn eq(&self, other: &Entry<K, V, D>) -> bool {
        self.key.eq(&other.key)
            && self.value.eq(&other.value)
            && self.deltas.len() == other.deltas.len()
            && self
                .deltas
                .iter()
                .zip(other.deltas.iter())
                .all(|(a, b)| a.eq(b))
    }
}

impl<K, V, D> Footprint for Entry<K, V, D>
where
    K: Footprint,
    V: Diff<Delta = D> + Footprint,
    D: Footprint,
{
    /// Return the previous versions of this entry as Deltas.
    fn footprint(&self) -> Result<isize> {
        use std::mem::size_of;

        // TODO: create a test case for footprint.
        let size = size_of::<Vec<Delta<<V as Diff>::Delta>>>();

        let mut size = self.key.footprint()?;
        size += self.value.footprint()?;
        for delta in self.deltas.iter() {
            size += delta.footprint()?;
        }
        Ok(size)
    }
}

impl<K, V, D> Entry<K, V, D>
where
    V: Diff<Delta = D>,
{
    pub const ID: u32 = ENTRY_VER;

    /// Create a new entry with key, value.
    pub fn new(key: K, value: V, seqno: u64) -> Entry<K, V, D> {
        Entry {
            key,
            value: Value::U { value, seqno },
            deltas: Vec::default(),
        }
    }

    /// Create a new entry that is marked as deleted.
    pub fn new_deleted(key: K, seqno: u64) -> Entry<K, V, D> {
        Entry {
            key,
            value: Value::D { seqno },
            deltas: Vec::default(),
        }
    }

    /// From a set of values, where values are different versions of same item,
    /// `values[0]` holding the oldest version and `values[n-1]` holding the latest
    /// version.
    pub fn from_values(key: K, mut values: Vec<Value<V>>) -> Result<Self>
    where
        V: Clone,
        <V as Diff>::Delta: From<V>,
    {
        if values.is_empty() {
            err_at!(InvalidInput, msg: "empty set of values for db::Entry")?
        }
        let mut entry = match values.remove(0) {
            Value::U { value, seqno } => Entry::new(key, value, seqno),
            Value::D { seqno } => Entry::new_deleted(key, seqno),
        };
        for value in values.into_iter() {
            match value {
                Value::U { value, seqno } => entry.insert(value, seqno),
                Value::D { seqno } => entry.delete(seqno),
            }
        }

        Ok(entry)
    }

    /// Insert a newer version for value. Older version shall be converted to delta.
    pub fn insert(&mut self, value: V, seqn: u64)
    where
        V: Clone,
    {
        let delta = match self.value.clone() {
            Value::U { value: oldv, seqno } => {
                let delta: <V as Diff>::Delta = value.diff(&oldv);
                Delta::U { delta, seqno }
            }
            Value::D { seqno } => Delta::D { seqno },
        };
        self.value = Value::U { value, seqno: seqn };
        self.deltas.push(delta);
    }

    /// Insert the newer version marked as deleted. Older version shall be converted
    /// to delta. Back-to-back deletes are not de-duplicated for the sake of seqno.
    pub fn delete(&mut self, seqn: u64)
    where
        V: Clone,
        <V as Diff>::Delta: From<V>,
    {
        match self.value.clone() {
            Value::U { value: oldv, seqno } => {
                self.value = Value::D { seqno: seqn };

                let delta: <V as Diff>::Delta = oldv.into();
                self.deltas.push(Delta::U { delta, seqno });
            }
            Value::D { seqno } => {
                self.value = Value::D { seqno: seqn };
                self.deltas.push(Delta::D { seqno });
            }
        };
    }

    /// Purge all deltas. Only the latest version will be available after this call.
    pub fn drain_deltas(&mut self) {
        self.deltas.drain(..);
    }
}

impl<K, V, D> Entry<K, V, D>
where
    V: Diff<Delta = D>,
{
    pub fn to_seqno(&self) -> u64 {
        match self.value {
            Value::U { seqno, .. } => seqno,
            Value::D { seqno } => seqno,
        }
    }

    pub fn to_key(&self) -> K
    where
        K: Clone,
    {
        self.key.clone()
    }

    pub fn to_value(&self) -> Option<V>
    where
        V: Clone,
    {
        match &self.value {
            Value::U { value, .. } => Some(value.clone()),
            Value::D { .. } => None,
        }
    }

    pub fn as_key(&self) -> &K {
        &self.key
    }

    pub fn borrow_key<Q>(&self) -> &Q
    where
        K: Borrow<Q>,
    {
        self.key.borrow()
    }

    pub fn is_deleted(&self) -> bool {
        match self.value {
            Value::U { .. } => false,
            Value::D { .. } => true,
        }
    }

    /// Return a list of all the versions of values, `values[0]` holds the oldest
    /// version `values[n-1]` holds the latest version.
    pub fn to_values(&self) -> Vec<Value<V>>
    where
        V: Clone,
        <V as Diff>::Delta: Clone,
    {
        let mut values = vec![self.value.clone()];
        let mut val: Option<V> = self.to_value();
        for d in self.deltas.iter().rev() {
            let (old, seqno): (Option<V>, u64) = match (val, d.clone()) {
                (Some(v), Delta::U { delta, seqno }) => (Some(v.merge(&delta)), seqno),
                (Some(_), Delta::D { seqno }) => (None, seqno),
                (None, Delta::U { delta, seqno }) => (Some(delta.into()), seqno),
                (None, Delta::D { seqno }) => (None, seqno),
            };
            values.push(
                old.clone()
                    .map(|value| Value::U { value, seqno })
                    .unwrap_or(Value::D { seqno }),
            );
            val = old;
        }

        values.reverse();

        values
    }

    /// Check whether all version of `other` is present in `self`.
    pub fn contains(&self, other: &Self) -> bool
    where
        V: Clone + PartialEq,
        <V as Diff>::Delta: Clone,
    {
        let values = self.to_values();
        other.to_values().iter().all(|v| values.contains(v))
    }

    /// Commit all versions from `other`, into `self`. Make sure versions in `other`
    /// and `self` are unique and exclusive.
    pub fn commit(&self, other: &Self) -> Self
    where
        K: PartialEq + Clone,
        V: Clone,
        <V as Diff>::Delta: Clone + From<V>,
    {
        if self.key != other.key {
            return self.clone();
        }

        match self.to_values() {
            values if values.is_empty() => other.clone(),
            mut values => {
                values.extend(other.to_values());
                values.sort_by_key(|v| v.to_seqno());
                Entry::from_values(self.key.clone(), values).ok().unwrap()
            }
        }
    }

    /// Compact entry based on Cutoff, refer to [Cutoff] type description for details.
    pub fn compact(mut self, cutoff: Cutoff) -> Option<Self>
    where
        Self: Sized,
    {
        let (val_seqno, deleted) = match &self.value {
            Value::U { seqno, .. } => (*seqno, false),
            Value::D { seqno } => (*seqno, true),
        };

        let cutoff = match cutoff {
            crate::db::Cutoff::Lsm(cutoff) => cutoff,
            crate::db::Cutoff::Mono if deleted => return None,
            crate::db::Cutoff::Mono => {
                self.deltas = vec![];
                return Some(self);
            }
            crate::db::Cutoff::Tombstone(cutoff) if deleted => match cutoff {
                Bound::Included(cutoff) if val_seqno <= cutoff => return None,
                Bound::Excluded(cutoff) if val_seqno < cutoff => return None,
                Bound::Unbounded => return None,
                _ => return Some(self),
            },
            crate::db::Cutoff::Tombstone(_) => return Some(self),
        };

        // lsm compact
        match cutoff {
            Bound::Included(std::u64::MIN) => return Some(self),
            Bound::Excluded(std::u64::MIN) => return Some(self),
            Bound::Included(cutoff) if val_seqno <= cutoff => return None,
            Bound::Excluded(cutoff) if val_seqno < cutoff => return None,
            Bound::Unbounded => return None,
            _ => (),
        }

        // Otherwise, purge only those versions that are before cutoff
        self.deltas = self
            .deltas
            .drain(..)
            .skip_while(|d| {
                let seqno = d.to_seqno();
                match cutoff {
                    Bound::Included(cutoff) if seqno <= cutoff => true,
                    Bound::Excluded(cutoff) if seqno < cutoff => true,
                    _ => false,
                }
            })
            .collect();

        Some(self)
    }
}

#[cfg(test)]
#[path = "entry_test.rs"]
mod entry_test;
