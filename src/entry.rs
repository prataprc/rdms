use std::{
    borrow::Borrow,
    convert::TryInto,
    fs,
    mem::ManuallyDrop,
    ops::Bound,
    sync::atomic::{AtomicBool, Ordering::SeqCst},
};

#[allow(unused_imports)]
use crate::{
    core::{Cutoff, Diff, Footprint, Result, Serialize},
    llrb::Llrb,
    mvcc::Mvcc,
    rdms::{self, Rdms},
    robt::Robt,
    scans,
    wal::Wal,
};
use crate::{error::Error, vlog};

#[derive(Clone)]
pub(crate) struct Delta<V>
where
    V: Clone + Diff,
{
    data: InnerDelta<V>,
}

/// Delta maintains the older version of value, with necessary fields for
/// log-structured-merge.
#[derive(Clone)]
pub(crate) enum InnerDelta<V>
where
    V: Clone + Diff,
{
    U { delta: vlog::Delta<V>, seqno: u64 },
    D { seqno: u64 },
}

// Delta construction methods.
impl<V> Delta<V>
where
    V: Clone + Diff,
{
    pub(crate) fn new_upsert(delta: vlog::Delta<V>, seqno: u64) -> Delta<V> {
        Delta {
            data: InnerDelta::U { delta, seqno },
        }
    }

    pub(crate) fn new_delete(seqno: u64) -> Delta<V> {
        Delta {
            data: InnerDelta::D { seqno },
        }
    }
}

impl<V> Footprint for Delta<V>
where
    V: Clone + Diff,
{
    fn footprint(&self) -> Result<isize> {
        use std::mem::size_of;

        let fp: isize = convert_at!(size_of::<Delta<V>>())?;
        Ok(fp
            + match &self.data {
                InnerDelta::U { delta, .. } => delta.footprint()?,
                InnerDelta::D { .. } => 0,
            })
    }
}

impl<V> AsRef<InnerDelta<V>> for Delta<V>
where
    V: Clone + Diff,
{
    fn as_ref(&self) -> &InnerDelta<V> {
        &self.data
    }
}

/// Delta accessor methods
impl<V> Delta<V>
where
    V: Clone + Diff,
{
    /// Return the underlying _difference_ value for this delta.
    #[cfg(test)]
    pub(crate) fn to_diff(&self) -> Option<<V as Diff>::D> {
        match &self.data {
            InnerDelta::D { .. } => None,
            InnerDelta::U { delta, .. } => delta.to_native_delta(),
        }
    }

    /// Return the underlying _difference_ value for this delta.
    #[cfg(test)]
    pub(crate) fn into_diff(self) -> Option<<V as Diff>::D> {
        match self.data {
            InnerDelta::D { .. } => None,
            InnerDelta::U { delta, .. } => delta.into_native_delta(),
        }
    }

    /// Return the seqno and the state of modification. `true` means
    /// this version was a create/update, and `false` means
    /// this version was deleted.
    #[cfg(test)]
    pub(crate) fn to_seqno_state(&self) -> (bool, u64) {
        match &self.data {
            InnerDelta::U { seqno, .. } => (true, *seqno),
            InnerDelta::D { seqno } => (false, *seqno),
        }
    }

    #[cfg(test)]
    pub(crate) fn into_upserted(self) -> Option<(vlog::Delta<V>, u64)> {
        match self.data {
            InnerDelta::U { delta, seqno } => Some((delta, seqno)),
            InnerDelta::D { .. } => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn into_deleted(self) -> Option<u64> {
        match self.data {
            InnerDelta::D { seqno } => Some(seqno),
            InnerDelta::U { .. } => None,
        }
    }

    /// Return the seqno at which this delta was modified,
    /// which includes Create and Delete operations.
    /// To differentiate between Create and Delete operations
    /// use born_seqno() and dead_seqno() methods respectively.
    pub(crate) fn to_seqno(&self) -> u64 {
        match &self.data {
            InnerDelta::U { seqno, .. } => *seqno,
            InnerDelta::D { seqno } => *seqno,
        }
    }

    pub(crate) fn is_reference(&self) -> bool {
        match self.data {
            InnerDelta::U {
                delta: vlog::Delta::Reference { .. },
                ..
            } => true,
            _ => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_deleted(&self) -> bool {
        match self.data {
            InnerDelta::D { .. } => true,
            InnerDelta::U { .. } => false,
        }
    }
}

pub(crate) enum Value<V> {
    U {
        value: ManuallyDrop<Box<vlog::Value<V>>>,
        is_reclaim: AtomicBool,
        seqno: u64,
    },
    D {
        seqno: u64,
    },
}

impl<V> Clone for Value<V>
where
    V: Clone,
{
    fn clone(&self) -> Value<V> {
        match self {
            Value::U {
                value,
                is_reclaim,
                seqno,
            } => Value::U {
                value: value.clone(),
                is_reclaim: AtomicBool::new(is_reclaim.load(SeqCst)),
                seqno: *seqno,
            },
            Value::D { seqno } => Value::D { seqno: *seqno },
        }
    }
}

impl<V> Drop for Value<V> {
    fn drop(&mut self) {
        // if is_reclaim is false, then it is a mvcc-clone. so don't touch
        // the value.
        match self {
            Value::U {
                value, is_reclaim, ..
            } if is_reclaim.load(SeqCst) => unsafe { ManuallyDrop::drop(value) },
            _ => (),
        }
    }
}

// Value construction methods
impl<V> Value<V>
where
    V: Clone,
{
    pub(crate) fn new_upsert(v: Box<vlog::Value<V>>, seqno: u64) -> Value<V> {
        Value::U {
            value: ManuallyDrop::new(v),
            is_reclaim: AtomicBool::new(true),
            seqno,
        }
    }

    pub(crate) fn new_upsert_value(value: V, seqno: u64) -> Value<V> {
        Value::U {
            value: ManuallyDrop::new(Box::new(vlog::Value::new_native(value))),
            is_reclaim: AtomicBool::new(true),
            seqno,
        }
    }

    pub(crate) fn new_delete(seqno: u64) -> Value<V> {
        Value::D { seqno }
    }

    pub(crate) fn mvcc_clone(&self, copyval: bool) -> Value<V> {
        match self {
            Value::U {
                value,
                seqno,
                is_reclaim,
            } if !copyval => {
                is_reclaim.store(false, SeqCst);
                let v = value.as_ref() as *const vlog::Value<V>;
                let value = unsafe { Box::from_raw(v as *mut vlog::Value<V>) };
                Value::U {
                    value: ManuallyDrop::new(value),
                    is_reclaim: AtomicBool::new(true),
                    seqno: *seqno,
                }
            }
            val => val.clone(),
        }
    }
}

// Value accessor methods
impl<V> Value<V>
where
    V: Clone,
{
    pub(crate) fn to_native_value(&self) -> Option<V> {
        match &self {
            Value::U { value, .. } => value.to_native_value(),
            Value::D { .. } => None,
        }
    }

    pub(crate) fn to_seqno(&self) -> u64 {
        match self {
            Value::U { seqno, .. } => *seqno,
            Value::D { seqno } => *seqno,
        }
    }

    pub(crate) fn is_deleted(&self) -> bool {
        match self {
            Value::U { .. } => false,
            Value::D { .. } => true,
        }
    }

    pub(crate) fn is_reference(&self) -> bool {
        match self {
            Value::U { value, .. } => value.is_reference(),
            _ => false,
        }
    }
}

impl<V> Footprint for Value<V>
where
    V: Footprint,
{
    fn footprint(&self) -> Result<isize> {
        use std::mem::size_of;

        Ok(match self {
            Value::U { value, .. } => {
                let size: isize = convert_at!(size_of::<V>())?;
                size + value.footprint()?
            }
            Value::D { .. } => 0,
        })
    }
}

/// Entry is the covering structure for a {Key, value} pair
/// indexed by rdms.
///
/// It is a user facing structure, also used in stitching together
/// different components of [Rdms].
#[derive(Clone)]
pub struct Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    key: K,
    value: Value<V>,
    deltas: Vec<Delta<V>>,
}

impl<K, V> Borrow<K> for Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn borrow(&self) -> &K {
        self.as_key()
    }
}

impl<K, V> Footprint for Entry<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Return the previous versions of this entry as Deltas.
    fn footprint(&self) -> Result<isize> {
        let mut fp = self.key.footprint()?;
        if !self.is_deleted() {
            fp += self.value.footprint()?;
        }
        for delta in self.deltas.iter() {
            fp += delta.footprint()?;
        }
        Ok(fp)
    }
}

// Entry construction methods.
impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Key's memory footprint cannot exceed this limit. _1GB_.
    pub const KEY_SIZE_LIMIT: usize = 1024 * 1024 * 1024;
    /// Value's memory footprint cannot exceed this limit. _1TB_.
    pub const VALUE_SIZE_LIMIT: usize = 1024 * 1024 * 1024 * 1024;
    /// Value diff's memory footprint cannot exceed this limit. _1TB_.
    pub const DIFF_SIZE_LIMIT: usize = 1024 * 1024 * 1024 * 1024;

    pub(crate) fn new(key: K, value: Value<V>) -> Entry<K, V> {
        Entry {
            key,
            value,
            deltas: vec![],
        }
    }

    pub(crate) fn mvcc_clone(&self, copyval: bool) -> Entry<K, V> {
        Entry {
            key: self.key.clone(),
            value: self.value.mvcc_clone(copyval),
            deltas: self.deltas.clone(),
        }
    }

    pub(crate) fn set_deltas(&mut self, deltas: Vec<Delta<V>>) {
        self.deltas = deltas;
    }
}

// Entry accessor methods.
impl<K, V> Entry<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    // Corresponds to CREATE and UPDATE operations also the latest version,
    // for this entry. In non-lsm mode this is equivalent to over-writing
    // previous value.
    //
    // `nentry` is new_entry to be CREATE/UPDATE into index.
    //
    // TODO: may be we can just pass the Value, instead of `nentry` ?
    pub(crate) fn prepend_version(&mut self, nentry: Self, lsm: bool) -> Result<isize> {
        if lsm {
            self.prepend_version_lsm(nentry)
        } else {
            self.prepend_version_nolsm(nentry)
        }
    }

    // `nentry` is new_entry to be CREATE/UPDATE into index.
    fn prepend_version_nolsm(&mut self, nentry: Self) -> Result<isize> {
        let size = self.value.footprint()?;
        self.value = nentry.value.clone();
        Ok(self.value.footprint()? - size)
    }

    // `nentry` is new_entry to be CREATE/UPDATE into index.
    fn prepend_version_lsm(&mut self, nentry: Self) -> Result<isize> {
        let delta = match &self.value {
            Value::D { seqno } => Ok(Delta::new_delete(*seqno)),
            Value::U { value, seqno, .. } if !value.is_reference() => {
                // compute delta
                match &nentry.value {
                    Value::D { .. } => {
                        let diff: <V as Diff>::D = {
                            let v = value.to_native_value().unwrap();
                            From::from(v)
                        };
                        {
                            let v = vlog::Delta::new_native(diff);
                            Ok(Delta::new_upsert(v, *seqno))
                        }
                    }
                    Value::U { value: nvalue, .. } => {
                        let dff = nvalue
                            .to_native_value()
                            .unwrap()
                            .diff(&value.to_native_value().unwrap());
                        {
                            let v = vlog::Delta::new_native(dff);
                            Ok(Delta::new_upsert(v, *seqno))
                        }
                    }
                }
            }
            Value::U { .. } => {
                //
                err_at!(Fatal, msg: format!("Entry.prepend_version_lsm()"))
            }
        }?;

        let size = {
            let size = nentry.value.footprint()? + delta.footprint()?;
            size - self.value.footprint()?
        };

        self.deltas.insert(0, delta);
        self.prepend_version_nolsm(nentry)?;

        Ok(size)
    }

    // DELETE operation, only in lsm-mode or sticky mode.
    pub(crate) fn delete(&mut self, seqno: u64) -> Result<isize> {
        let size = self.footprint()?;

        match &self.value {
            Value::D { seqno } => {
                // insert a delete delta
                self.deltas.insert(0, Delta::new_delete(*seqno));
                Ok(())
            }
            Value::U { value, seqno, .. } if !value.is_reference() => {
                let delta = {
                    let d: <V as Diff>::D = From::from(value.to_native_value().unwrap());
                    vlog::Delta::new_native(d)
                };
                self.deltas.insert(0, Delta::new_upsert(delta, *seqno));
                Ok(())
            }
            Value::U { .. } => err_at!(Fatal, msg: format!("Entry.delete()")),
        }?;

        self.value = Value::new_delete(seqno);
        Ok(self.footprint()? - size)
    }
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // purge all versions whose seqno <= or < `cutoff`.
    pub(crate) fn purge(mut self, cutoff: Cutoff) -> Option<Entry<K, V>> {
        let n = self.to_seqno();

        let cutoff = match cutoff {
            Cutoff::Mono if self.is_deleted() => return None,
            Cutoff::Mono => {
                self.set_deltas(vec![]);
                return Some(self);
            }
            Cutoff::Lsm(cutoff) => cutoff,
            Cutoff::Tombstone(cutoff) if self.is_deleted() => match cutoff {
                Bound::Included(cutoff) if n <= cutoff => return None,
                Bound::Excluded(cutoff) if n < cutoff => return None,
                Bound::Unbounded => return None,
                _ => return Some(self),
            },
            Cutoff::Tombstone(_) => return Some(self),
        };

        // If all versions of this entry are before cutoff, then purge entry
        match cutoff {
            Bound::Included(std::u64::MIN) => return Some(self),
            Bound::Excluded(std::u64::MIN) => return Some(self),
            Bound::Included(cutoff) if n <= cutoff => return None,
            Bound::Excluded(cutoff) if n < cutoff => return None,
            Bound::Unbounded => return None,
            _ => (),
        }
        // Otherwise, purge only those versions that are before cutoff
        self.deltas = self
            .deltas
            .drain(..)
            .take_while(|d| {
                let seqno = d.to_seqno();
                match cutoff {
                    Bound::Included(cutoff) if seqno > cutoff => true,
                    Bound::Excluded(cutoff) if seqno >= cutoff => true,
                    _ => false,
                }
            })
            .collect();
        Some(self)
    }
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Pick all versions whose seqno is within the specified range.
    /// Note that, by rdms-design only memory-indexes ingesting new
    /// mutations are subjected to this filter function.
    pub fn filter_within(
        &self,
        start: Bound<u64>, // filter from
        end: Bound<u64>,   // filter till
    ) -> Option<Entry<K, V>> {
        // skip versions newer than requested range.
        let entry = self.skip_till(start.clone(), end)?;
        // purge versions older than request range.
        match start {
            Bound::Included(x) => {
                let cutoff = Cutoff::new_lsm(Bound::Excluded(x));
                entry.purge(cutoff)
            }
            Bound::Excluded(x) => {
                let cutoff = Cutoff::new_lsm(Bound::Included(x));
                entry.purge(cutoff)
            }
            Bound::Unbounded => Some(entry),
        }
    }

    fn skip_till(&self, ob: Bound<u64>, nb: Bound<u64>) -> Option<Entry<K, V>> {
        // skip entire entry if it is before the specified range.
        let n = self.to_seqno();
        match ob {
            Bound::Included(o_seqno) if n < o_seqno => return None,
            Bound::Excluded(o_seqno) if n <= o_seqno => return None,
            _ => (),
        }
        // skip the entire entry if it is after the specified range.
        let o = self.deltas.last().map_or(n, |d| d.to_seqno());
        match nb {
            Bound::Included(nb) if o > nb => return None,
            Bound::Excluded(nb) if o >= nb => return None,
            Bound::Included(nb) if n <= nb => return Some(self.clone()),
            Bound::Excluded(nb) if n < nb => return Some(self.clone()),
            Bound::Unbounded => return Some(self.clone()),
            _ => (),
        };

        // println!("skip_till {} {} {:?}", o, n, nb);
        // partial skip.
        let mut entry = self.clone();
        let mut iter = entry.deltas.drain(..);
        while let Some(delta) = iter.next() {
            let (value, _) = next_value(entry.value.to_native_value(), delta.data);
            entry.value = value;
            let seqno = entry.value.to_seqno();
            let done = match nb {
                Bound::Included(n_seqno) if seqno <= n_seqno => true,
                Bound::Excluded(n_seqno) if seqno < n_seqno => true,
                _ => false,
            };
            // println!("skip_till loop {} {:?} {} ", seqno, nb, done);
            if done {
                // collect the remaining deltas and return
                entry.deltas = iter.collect();
                // println!("skip_till fin {}", entry.deltas.len());
                return Some(entry);
            }
        }

        None
    }

    /// Return an iterator for all existing versions for this entry.
    pub fn versions(&self) -> VersionIter<K, V> {
        VersionIter {
            key: self.key.clone(),
            entry: Some(Entry {
                key: self.key.clone(),
                value: self.value.clone(),
                deltas: Default::default(),
            }),
            curval: None,
            deltas: Some(self.to_deltas().into_iter()),
        }
    }
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    /// Merge two version chain for same key. This can happen between
    /// two entries from two index, where one of them is a newer snapshot
    /// of the same index. In any case it is expected that all versions of
    /// one entry shall be greater than all versions of the other entry.
    pub fn xmerge(self, entry: Entry<K, V>) -> Result<Entry<K, V>> {
        // `a` is newer than `b`, and all versions in a and b are mutually
        // exclusive in seqno ordering.
        let (a, mut b) = if self.to_seqno() > entry.to_seqno() {
            Ok((self, entry))
        } else if entry.to_seqno() > self.to_seqno() {
            Ok((entry, self))
        } else {
            let msg = format!("{} == {}", entry.to_seqno(), self.to_seqno());
            err_at!(Fatal, msg: msg)
        }?;

        if cfg!(debug_assertions) {
            a.validate_xmerge(&b)?;
        }

        for ne in a.versions().collect::<Vec<Entry<K, V>>>().into_iter().rev() {
            // println!("xmerge {} {}", ne.to_seqno(), ne.is_deleted());
            b.prepend_version(ne, true /* lsm */)?;
        }
        Ok(b)
    }

    // `self` is newer than `entry`
    fn validate_xmerge(&self, entr: &Entry<K, V>) -> Result<()> {
        // validate ordering
        let mut seqnos = vec![self.to_seqno()];
        self.deltas.iter().for_each(|d| seqnos.push(d.to_seqno()));
        seqnos.push(entr.to_seqno());
        entr.deltas.iter().for_each(|d| seqnos.push(d.to_seqno()));
        let fail = seqnos[0..seqnos.len() - 1]
            .into_iter()
            .zip(seqnos[1..].into_iter())
            .any(|(a, b)| a <= b);

        if fail {
            //println!(
            //    "validate_xmerge {:?} {} {:?} {} {:?}",
            //    seqnos,
            //    self.to_seqno(),
            //    self.deltas
            //        .iter()
            //        .map(|d| d.to_seqno())
            //        .collect::<Vec<u64>>(),
            //    entr.to_seqno(),
            //    entr.deltas
            //        .iter()
            //        .map(|d| d.to_seqno())
            //        .collect::<Vec<u64>>(),
            //);
            err_at!(Fatal, msg: format!("Entry.validate_xmerge()"))
        } else {
            Ok(())
        }
    }
}

impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Default + Clone + Diff + Serialize,
    <V as Diff>::D: Default + Serialize,
{
    pub(crate) fn fetch_value(&mut self, fd: &mut fs::File) -> Result<()> {
        Ok(match &self.value {
            Value::U { value, seqno, .. } => match value.to_reference() {
                Some((fpos, len, _seqno)) => {
                    self.value = Value::new_upsert(
                        Box::new(vlog::fetch_value(fpos, len, fd)?),
                        *seqno,
                    );
                }
                _ => (),
            },
            _ => (),
        })
    }

    pub(crate) fn fetch_deltas(&mut self, fd: &mut fs::File) -> Result<()> {
        for delta in self.deltas.iter_mut() {
            match delta.data {
                InnerDelta::U {
                    delta: vlog::Delta::Reference { fpos, length, .. },
                    seqno,
                } => {
                    *delta =
                        Delta::new_upsert(vlog::fetch_delta(fpos, length, fd)?, seqno);
                }
                _ => (),
            }
        }
        Ok(())
    }
}

// Entry accessor methods
impl<K, V> Entry<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    /// Return a reference to key.
    #[inline]
    pub fn as_key(&self) -> &K {
        &self.key
    }

    /// Return owned key vlalue.
    #[inline]
    pub fn to_key(&self) -> K {
        self.key.clone()
    }

    #[inline]
    pub(crate) fn as_deltas(&self) -> &Vec<Delta<V>> {
        &self.deltas
    }

    pub(crate) fn to_delta_count(&self) -> usize {
        self.deltas.len()
    }

    pub(crate) fn as_value(&self) -> &Value<V> {
        &self.value
    }

    /// Return the previous versions of this entry as Deltas.
    #[inline]
    pub(crate) fn to_deltas(&self) -> Vec<Delta<V>> {
        self.deltas.clone()
    }

    /// Return value. If entry is marked as deleted, return None.
    pub fn to_native_value(&self) -> Option<V> {
        self.value.to_native_value()
    }

    /// Return the latest seqno that created/updated/deleted this entry.
    #[inline]
    pub fn to_seqno(&self) -> u64 {
        match self.value {
            Value::U { seqno, .. } => seqno,
            Value::D { seqno, .. } => seqno,
        }
    }

    /// Return the seqno and the state of modification. `true` means
    /// latest value was a create/update, and `false` means latest value
    /// was deleted.
    #[inline]
    pub fn to_seqno_state(&self) -> (bool, u64) {
        match self.value {
            Value::U { seqno, .. } => (true, seqno),
            Value::D { seqno, .. } => (false, seqno),
        }
    }

    /// Return whether this entry is in deleted state, applicable onle
    /// in lsm mode.
    pub fn is_deleted(&self) -> bool {
        self.value.is_deleted()
    }
}

/// Iterate from newest to oldest _available_ version for this entry.
pub struct VersionIter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    key: K,
    entry: Option<Entry<K, V>>,
    curval: Option<V>,
    deltas: Option<std::vec::IntoIter<Delta<V>>>,
}

impl<K, V> Iterator for VersionIter<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        // first iteration
        if let Some(entry) = self.entry.take() {
            if entry.value.is_reference() {
                self.deltas.take();
                return None;
            } else {
                self.curval = entry.to_native_value();
                return Some(entry);
            }
        }
        // remaining iterations
        let delta = {
            match &mut self.deltas {
                Some(deltas) => match deltas.next() {
                    None => {
                        return None;
                    }
                    Some(delta) if delta.is_reference() => {
                        self.deltas.take();
                        return None;
                    }
                    Some(delta) => delta,
                },
                None => return None,
            }
        };
        let (value, curval) = next_value(self.curval.take(), delta.data);
        self.curval = curval;
        Some(Entry::new(self.key.clone(), value))
    }
}

fn next_value<V>(value: Option<V>, delta: InnerDelta<V>) -> (Value<V>, Option<V>)
where
    V: Clone + Diff,
{
    match (value, delta) {
        (None, InnerDelta::D { seqno }) => {
            // consequitive delete
            (Value::new_delete(seqno), None)
        }
        (Some(_), InnerDelta::D { seqno }) => {
            // this entry is deleted.
            (Value::new_delete(seqno), None)
        }
        (None, InnerDelta::U { delta, seqno }) => {
            // previous entry was a delete.
            let nv: V = From::from(delta.into_native_delta().unwrap());
            let value =
                Value::new_upsert(Box::new(vlog::Value::new_native(nv.clone())), seqno);
            (value, Some(nv))
        }
        (Some(curval), InnerDelta::U { delta, seqno }) => {
            // this and previous entry are create/update.
            let nv = curval.merge(&delta.into_native_delta().unwrap());
            let value =
                Value::new_upsert(Box::new(vlog::Value::new_native(nv.clone())), seqno);
            (value, Some(nv))
        }
    }
}
