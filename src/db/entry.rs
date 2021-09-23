use cbordata::Cborize;

use std::{borrow::Borrow, convert::TryFrom, fmt, ops::Bound, result};

use crate::{
    db::{Cutoff, Delta, Diff, Footprint, Value},
    Error, Result,
};

const ENTRY_VER: u32 = 0x00050001;

// TODO: test case for Cborize

/// Entry type, describe a single `{key,value}` entry within indexed data-set.
// NOTE:
// Deriving Cborize on Entry has a problem, `deltas` field is using an
// associated type of Diff trait which is declared as constraint no `V`. Now
// automatically detecting this and deriving FromCbor and IntoCbor for
// <V as Diff>::Delta seem to be difficult.
//
// We are using a sleek idea, we add the type parameter `D` to Entry but default
// it to <V as Diff>::Delta, so that rest of the package and outside crates can
// simple use Entry<K, V>.
#[derive(Clone, Cborize)]
pub struct Entry<K, V, D = <V as Diff>::Delta>
where
    V: Diff<Delta = D>,
{
    pub key: K,
    pub value: Value<V>,
    pub deltas: Vec<Delta<D>>, // from oldest to newest
}

impl<K, V, D> fmt::Debug for Entry<K, V, D>
where
    K: fmt::Debug,
    V: fmt::Debug + Diff<Delta = D>,
    D: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}-{:?}-{:?}", self.key, self.value, self.deltas)
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

impl<K, V, D> Borrow<K> for Entry<K, V, D>
where
    V: Diff<Delta = D>,
{
    fn borrow(&self) -> &K {
        self.as_key()
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

        let mut size = {
            let n = size_of::<Entry<K, V, D>>() - size_of::<K>() - size_of::<Value<V>>();
            err_at!(FailConvert, isize::try_from(n))?
        };
        size += self.key.footprint()?;
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

    /// Start a new entry in upsert state.
    pub fn new(key: K, value: V, seqno: u64) -> Entry<K, V, D> {
        Entry {
            key,
            value: Value::new_upsert(value, seqno),
            deltas: Vec::default(),
        }
    }

    /// Start a new entry in deleted state.
    pub fn new_delete(key: K, seqno: u64) -> Entry<K, V, D> {
        Entry {
            key,
            value: Value::new_delete(seqno),
            deltas: Vec::default(),
        }
    }

    /// From a set of values, where values are different versions of same item,
    /// `values[0]` holding the oldest version and `values[n-1]` holding the latest
    /// version.
    pub fn from_values(key: K, mut values: Vec<Value<V>>) -> Result<Self>
    where
        K: Clone,
        D: Clone + From<V>,
    {
        if values.is_empty() {
            err_at!(InvalidInput, msg: "empty set of values for db::Entry")?
        }
        let mut entry = match values.remove(0) {
            Value::U { value, seqno } => Entry::new(key, value, seqno),
            Value::D { seqno } => Entry::new_delete(key, seqno),
        };
        for value in values.into_iter() {
            entry = match value {
                Value::U { value, seqno } => entry.insert(value, seqno),
                Value::D { seqno } => entry.delete(seqno),
            }
        }

        Ok(entry)
    }

    /// Insert a newer version of value. Older version shall be converted to delta.
    pub fn insert(&self, value: V, seqno: u64) -> Entry<K, V, D>
    where
        K: Clone,
        D: Clone,
    {
        let delta = match self.value.clone() {
            Value::U {
                value: oval,
                seqno: oseq,
            } => {
                let delta: D = value.diff(&oval);
                Delta::new_upsert(delta, oseq)
            }
            Value::D { seqno: oseq } => Delta::new_delete(oseq),
        };

        let key = self.key.clone();
        let value = Value::U { value, seqno };
        let mut deltas = self.deltas.clone();
        deltas.push(delta);
        Entry { key, value, deltas }
    }

    /// Insert the newer version marked as deleted. Older version shall be converted
    /// to delta. Back-to-back deletes are not de-duplicated for the sake of
    /// seqno-consistency.
    pub fn delete(&self, seqno: u64) -> Entry<K, V, D>
    where
        K: Clone,
        D: Clone + From<V>,
    {
        let delta = match self.value.clone() {
            Value::U {
                value: oldv,
                seqno: oseq,
            } => {
                let delta: D = oldv.into();
                Delta::new_upsert(delta, oseq)
            }
            Value::D { seqno: oseq } => Delta::new_delete(oseq),
        };

        let key = self.key.clone();
        let value = Value::D { seqno };
        let mut deltas = self.deltas.clone();
        deltas.push(delta);
        Entry { key, value, deltas }
    }

    /// Purge all deltas. Only the latest version will be available after this call.
    pub fn drain_deltas(&self) -> Entry<K, V>
    where
        K: Clone,
    {
        Entry {
            key: self.key.clone(),
            value: self.value.clone(),
            deltas: Vec::default(),
        }
    }

    /// Commit all versions from `other`, into `self`. Make sure versions in `other`
    /// and `self` are unique and exclusive.
    pub fn commit(&self, other: &Self) -> Result<Self>
    where
        K: PartialEq + Clone,
        D: Clone + From<V>,
    {
        if self.key != other.key {
            err_at!(InvalidInput, msg: "commit entry with a different key value")?;
        }

        let entry = match self.to_values() {
            values if values.is_empty() => other.clone(),
            mut values => {
                values.extend(other.to_values());
                values.sort_by_key(|v| v.to_seqno());
                Entry::from_values(self.key.clone(), values)?
            }
        };

        Ok(entry)
    }

    /// Compact entry based on Cutoff, refer to [Cutoff] type description for details.
    pub fn compact(&self, cutoff: Cutoff) -> Option<Self>
    where
        K: Clone,
        D: Clone,
    {
        if cutoff.is_noop() {
            return Some(self.clone());
        }

        let (seqno, value) = self.value.unpack();

        let cutoff = match cutoff {
            // mono: return early.
            crate::db::Cutoff::Mono if value.is_none() => return None,
            crate::db::Cutoff::Mono => return Some(self.drain_deltas()),
            // tombstone: return early.
            crate::db::Cutoff::Tombstone(cutoff) if value.is_none() => match cutoff {
                Bound::Included(cutoff) if seqno <= cutoff => return None,
                Bound::Excluded(cutoff) if seqno < cutoff => return None,
                Bound::Unbounded => return None,
                _ => return Some(self.clone()),
            },
            crate::db::Cutoff::Tombstone(_) => return Some(self.clone()),
            // lsm: return / fall through
            crate::db::Cutoff::Lsm(cutoff) => match cutoff {
                // lsm: return early
                Bound::Included(cutoff) if seqno <= cutoff => return None,
                Bound::Excluded(cutoff) if seqno < cutoff => return None,
                Bound::Unbounded => return None,
                // lsm: fall through
                cutoff => cutoff,
            },
        };

        // Otherwise, purge only those versions that are before cutoff
        let deltas: Vec<Delta<D>> = self
            .deltas
            .iter()
            .skip_while(|d| {
                let seqno = d.to_seqno();
                match cutoff {
                    Bound::Included(cutoff) if seqno <= cutoff => true,
                    Bound::Excluded(cutoff) if seqno < cutoff => true,
                    _ => false,
                }
            })
            .map(Clone::clone)
            .collect();

        let entry = Entry {
            key: self.key.clone(),
            value: self.value.clone(),
            deltas,
        };
        Some(entry)
    }
}

impl<K, V, D> Entry<K, V, D>
where
    V: Diff<Delta = D>,
{
    /// Return the seqno for the latest version of this entry.
    pub fn to_seqno(&self) -> u64 {
        match self.value {
            Value::U { seqno, .. } => seqno,
            Value::D { seqno } => seqno,
        }
    }

    /// Return the entry key.
    pub fn to_key(&self) -> K
    where
        K: Clone,
    {
        self.key.clone()
    }

    /// Return the latest version of value.
    pub fn to_value(&self) -> Option<V> {
        match &self.value {
            Value::U { value, .. } => Some(value.clone()),
            Value::D { .. } => None,
        }
    }

    /// Return a reference to key
    pub fn as_key(&self) -> &K {
        &self.key
    }

    /// Borrow entry as Q.
    pub fn borrow_key<Q>(&self) -> &Q
    where
        K: Borrow<Q>,
    {
        self.key.borrow()
    }

    /// Return whether entry is marked as deleted
    pub fn is_deleted(&self) -> bool {
        match self.value {
            Value::U { .. } => false,
            Value::D { .. } => true,
        }
    }

    /// Return a list of all the versions of values, `values[0]` hold the oldest
    /// version `values[n-1]` hold the latest version.
    pub fn to_values(&self) -> Vec<Value<V>>
    where
        D: Clone,
    {
        let mut values = vec![self.value.clone()];
        let mut val: Option<V> = self.to_value();

        for d in self.deltas.iter().rev() {
            let (old, seqno): (Option<V>, u64) = match (val, d) {
                (Some(v), Delta::U { delta, seqno }) => (Some(v.merge(&delta)), *seqno),
                (Some(_), Delta::D { seqno }) => (None, *seqno),
                (None, Delta::U { delta, seqno }) => (Some(delta.clone().into()), *seqno),
                (None, Delta::D { seqno }) => (None, *seqno),
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
        K: PartialEq,
        V: PartialEq,
        D: Clone,
    {
        match self.key == other.key {
            false => false,
            true => {
                let values = self.to_values();
                other.to_values().iter().all(|v| values.contains(v))
            }
        }
    }

    /// Return the seqno for oldest version
    pub fn oldest_seqno(&self) -> u64 {
        self.deltas
            .first()
            .map(|x| x.to_seqno())
            .unwrap_or(self.value.to_seqno())
    }
}

#[cfg(test)]
#[path = "entry_test.rs"]
mod entry_test;
