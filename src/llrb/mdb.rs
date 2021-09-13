// Module ``mvcc`` implement [Multi-Version-Concurrency-Control][mvcc]
// variant of [Llrb].
//
// [Mvcc] type allow concurrent read and write access at API level,
// while behind the scenes, all write-operations are serialized into
// single thread, but the key difference is that [Mvcc] index allow
// concurrent-reads without using locks. To serialize concurrent writes
// [Mvcc] uses a spin-lock implementation that can be configured to
// _yield_ or _spin_ while waiting for the lock.
//
// **[LSM mode]**: Mvcc index can support log-structured-merge while
// mutating the tree. In simple terms, this means that nothing shall be
// over-written in the tree and all the mutations for the same key shall
// be preserved until they are purged.
//
// **Possible ways to configure Mvcc**:
//
// *spinlatch*, relevant only in multi-threaded context. Calling
// _set_spinlatch()_ with _true_ will have the calling thread to spin
// while waiting to acquire the lock. Calling it with _false_ will have the
// calling thread to yield to OS scheduler while waiting to acquire the lock.
//
// *seqno*, application can set the beginning sequence number before
// ingesting data into the index.
//
// [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
// [mvcc]: https://en.wikipedia.org/wiki/Multiversion_concurrency_control
// [LSM mode]: https://en.wikipedia.org/wiki/Log-structured_merge-tree

use mkit::{data::Diff, db, spinlock::Spinlock};

use std::{
    borrow::Borrow,
    cmp::{self, Ordering},
    fmt, marker,
    ops::{Bound, RangeBounds},
    sync::{Arc, Mutex},
};

use crate::{mdb_node::Node, mdb_op::Write, Error, Result};

pub const MAX_TREE_DEPTH: usize = 100;

macro_rules! compute_n_count {
    ($old:expr, $olde:expr) => {
        $old + $olde.map(|_| 0).unwrap_or(1)
    };
}

macro_rules! compute_n_deleted {
    ($old:expr, $olde:expr) => {
        $old.saturating_sub(
            $olde
                .map(|e| if e.is_deleted() { 1 } else { 0 })
                .unwrap_or(0),
        )
    };
}

/// Mdb type for thread-safe, concurrent reads and serialized writes.
///
/// [mvcc]: https://en.wikipedia.org/wiki/Multiversion_concurrency_control
/// [llrb]: https://en.wikipedia.org/wiki/Left-leaning_red-black_tree
#[derive(Clone)]
pub struct Mdb<K, V, D> {
    name: String,

    mu: Arc<Mutex<u32>>,
    inner: Arc<Spinlock<Arc<Inner<K, V, D>>>>,
}

impl<K, V, D> Mdb<K, V, D> {
    pub fn new(name: &str) -> Mdb<K, V, D> {
        let inner = Inner {
            root: None,
            seqno: 0,
            n_count: 0,
            n_deleted: 0,
        };

        Mdb {
            name: name.to_string(),
            mu: Arc::new(Mutex::new(0)),
            inner: Arc::new(Spinlock::new(Arc::new(inner))),
        }
    }
}

impl<K, V, D> Mdb<K, V, D> {
    /// Return number of entries in this instance.
    pub fn len(&self) -> usize {
        let inner = Arc::clone(&self.inner.read());
        inner.n_count
    }

    /// Return number of entries marked as deleted.
    pub fn deleted_count(&self) -> usize {
        let inner = Arc::clone(&self.inner.read());
        inner.n_deleted
    }

    /// Return whether index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return current sequence-no for index.
    pub fn to_seqno(&self) -> u64 {
        let inner = Arc::clone(&self.inner.read());
        inner.seqno
    }

    /// Update index to a new sequence-no, future mutations shall start
    /// from this value.
    pub fn set_seqno(&self, seqno: u64) -> u64
    where
        K: Clone,
        V: Clone,
        D: Clone,
    {
        let (mut inner, old_seqno) = {
            let inner = Arc::clone(&self.inner.read());
            (inner.as_ref().clone(), inner.seqno)
        };

        inner.seqno = seqno;
        *self.inner.write() = Arc::new(inner);

        old_seqno
    }

    /// Identify this index instance.
    #[inline]
    pub fn to_name(&self) -> String {
        self.name.clone()
    }
}

/// Result type for all write operations into index.
pub struct Wr<K, V, D> {
    /// Mutation sequence number for this write-operation.
    pub seqno: u64,
    pub old_entry: Option<db::Entry<K, V, D>>,
}

impl<K, V, D> Mdb<K, V, D> {
    /// Set `key`, `value` into index. If an older entry exist with same key,
    /// it shall be overwritten.
    pub fn set(&self, key: K, value: V) -> Result<Wr<K, V, D>>
    where
        K: Clone + Ord,
        V: Clone,
        D: Clone,
    {
        let _w = self.mu.lock();

        let inner = Arc::clone(&self.inner.read());
        let op = (key, value, None, None);
        let (inner, old_entry) = inner.set(op)?.into_root();
        let seqno = inner.seqno;
        *self.inner.write() = Arc::new(inner);

        Ok(Wr { seqno, old_entry })
    }

    /// Set `key`, `value` into index. If already an entry in present with
    /// `key`, `cas` should match entry's sequence-number. If index don't
    /// have an entry with `key`, `cas` must be ZERO.
    pub fn set_cas(&self, key: K, value: V, cas: u64) -> Result<Wr<K, V, D>>
    where
        K: Clone + Ord,
        V: Clone,
        D: Clone,
    {
        let _w = self.mu.lock();

        let inner = Arc::clone(&self.inner.read());
        let op = (key, value, Some(cas), None);
        let (inner, old_entry) = inner.set(op)?.into_root();
        let seqno = inner.seqno;
        *self.inner.write() = Arc::new(inner);

        Ok(Wr { seqno, old_entry })
    }

    /// Insert `key`, `value` into index. Non destructive version of
    /// set method. If an older entry exist with same key, use [Diff]
    /// to compute the delta and insert a new value-version.
    pub fn insert(&self, key: K, value: V) -> Result<Wr<K, V, D>>
    where
        K: Clone + Ord,
        V: Clone + Diff<Delta = D>,
        D: Clone,
    {
        let _w = self.mu.lock();

        let inner = Arc::clone(&self.inner.read());
        let op = (key, value, None, None);
        let (inner, old_entry) = inner.insert(op)?.into_root();
        let seqno = inner.seqno;
        *self.inner.write() = Arc::new(inner);

        Ok(Wr { seqno, old_entry })
    }

    /// Insert `key`, `value` into index. Non destructive version of
    /// set method. If an older entry exist with same `key`, use [Diff]
    /// to compute the delta and insert a new value-version. Also if an
    /// older entry exist with same `key`, `cas` should match entry's
    /// sequence-number.
    pub fn insert_cas(&self, key: K, value: V, cas: u64) -> Result<Wr<K, V, D>>
    where
        K: Clone + Ord,
        V: Clone + Diff<Delta = D>,
        D: Clone,
    {
        let _w = self.mu.lock();

        let inner = Arc::clone(&self.inner.read());
        let op = (key, value, Some(cas), None);
        let (inner, old_entry) = inner.insert(op)?.into_root();
        let seqno = inner.seqno;
        *self.inner.write() = Arc::new(inner);

        Ok(Wr { seqno, old_entry })
    }

    /// Remove the entry, matching the key, from the index.
    pub fn remove<Q>(&self, key: &Q) -> Result<Wr<K, V, D>>
    where
        K: Clone + Ord + Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
        V: Clone,
        D: Clone,
    {
        let _w = self.mu.lock();

        let inner = Arc::clone(&self.inner.read());
        let op = (key, None, None);
        let (inner, old_entry) = inner.remove(op)?.into_root();
        let seqno = inner.seqno;
        *self.inner.write() = Arc::new(inner);

        Ok(Wr { seqno, old_entry })
    }

    /// Remove the entry, with matching key and matching entry's
    /// sequencey-number with `cas`.
    pub fn remove_cas<Q>(&self, key: &Q, cas: u64) -> Result<Wr<K, V, D>>
    where
        K: Clone + Ord + Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
        V: Clone,
        D: Clone,
    {
        let _w = self.mu.lock();

        let inner = Arc::clone(&self.inner.read());
        let op = (key, Some(cas), None);
        let (inner, old_entry) = inner.remove(op)?.into_root();
        let seqno = inner.seqno;
        *self.inner.write() = Arc::new(inner);

        Ok(Wr { seqno, old_entry })
    }

    /// Non destructive version of remove method. Mark entry as deleted.
    pub fn delete<Q>(&self, key: &Q) -> Result<Wr<K, V, D>>
    where
        K: Clone + Ord + Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
        V: Clone + Diff<Delta = D>,
        D: Clone,
        <V as Diff>::Delta: From<V>,
    {
        let _w = self.mu.lock();

        let inner = Arc::clone(&self.inner.read());
        let op = (key, None, None);
        let (inner, old_entry) = inner.delete(op)?.into_root();
        let seqno = inner.seqno;
        *self.inner.write() = Arc::new(inner);

        Ok(Wr { seqno, old_entry })
    }

    /// Non destructive version of remove method. Mark entry as deleted.
    pub fn delete_cas<Q>(&self, key: &Q, cas: u64) -> Result<Wr<K, V, D>>
    where
        K: Clone + Ord + Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
        V: Clone + Diff<Delta = D>,
        D: Clone,
        <V as Diff>::Delta: From<V>,
    {
        let _w = self.mu.lock();

        let inner = Arc::clone(&self.inner.read());
        let op = (key, Some(cas), None);
        let (inner, old_entry) = inner.delete(op)?.into_root();
        let seqno = inner.seqno;
        *self.inner.write() = Arc::new(inner);

        Ok(Wr { seqno, old_entry })
    }

    /// Apply op on top of this index. For more detail refer to [Write] type.
    pub fn write(&self, op: Write<K, V>) -> Result<Wr<K, V, D>>
    where
        K: Clone + Ord,
        V: Clone + Diff<Delta = D>,
        D: Clone,
        <V as Diff>::Delta: From<V>,
    {
        let _w = self.mu.lock();

        let inner = Arc::clone(&self.inner.read());
        let (inner, old_entry) = match op {
            Write::Set {
                key,
                value,
                cas,
                seqno,
            } => inner.set((key, value, cas, seqno))?.into_root(),
            Write::Ins {
                key,
                value,
                cas,
                seqno,
            } => inner.insert((key, value, cas, seqno))?.into_root(),
            Write::Rem { key, cas, seqno } => {
                inner.remove((&key, cas, seqno))?.into_root()
            }
            Write::Del { key, cas, seqno } => {
                inner.delete((&key, cas, seqno))?.into_root()
            }
        };

        let seqno = inner.seqno;
        *self.inner.write() = Arc::new(inner);

        Ok(Wr { seqno, old_entry })
    }

    /// Commit a latest batch of mutations into this snapshot, there by
    /// creating a new snapshot. It is pre-requisite that the new batch
    /// of mutation and its seqno must all be newer than this index
    /// snapshot's latest seqno.
    pub fn commit<I>(&self, iter: I) -> Result<usize>
    where
        K: Clone + PartialEq + Ord,
        V: Clone + Diff<Delta = D>,
        D: Clone + From<V>,
        I: Iterator<Item = db::Entry<K, V, D>>,
    {
        let mut inner = self.inner.write();
        let (new_inner, n) = {
            let (ir, n) = inner.commit(iter)?;
            let (new_inner, _) = ir.into_root();
            (new_inner, n)
        };
        *inner = Arc::new(new_inner);

        Ok(n)
    }
}

impl<K, V, D> Mdb<K, V, D> {
    /// Get entry from index for `key`. If key is not found return
    /// [Error::KeyNotFound]
    pub fn get<Q>(&self, key: &Q) -> Result<db::Entry<K, V, D>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord + ?Sized,
    {
        let inner = Arc::clone(&self.inner.read());
        inner.get(key)
    }

    /// For full table scan.
    pub fn iter(&self) -> Result<Iter<K, V, D>> {
        let inner = Arc::clone(&self.inner.read());
        Ok(inner.iter())
    }

    /// Iterate over entries within the specifed `range`.
    pub fn range<R, Q>(&self, range: R) -> Result<Range<K, V, D, R, Q>>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        let inner = Arc::clone(&self.inner.read());
        Ok(inner.range(range))
    }

    /// Reverse iterate over entries withing specified `range`. While
    /// `range` method iterate entries from start_bound to end_bound
    /// `reverse` method iterate entries from end_bound to start_bound.
    pub fn reverse<R, Q>(&self, range: R) -> Result<Reverse<K, V, D, R, Q>>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        let inner = Arc::clone(&self.inner.read());
        Ok(inner.reverse(range))
    }

    /// Validate Mdb tree with following rules:
    ///
    /// * Root node is always black in color.
    /// * Verify the sort order between a node and its left/right child.
    /// * No node has RIGHT RED child and LEFT BLACK child (or NULL child).
    /// * Make sure there are no consecutive reds.
    /// * Make sure number of blacks are same on both left and right arm.
    /// * Make sure that the maximum depth do not exceed MAX_TREE_DEPTH.
    pub fn validate(&self) -> Result<()>
    where
        K: Ord + fmt::Debug,
    {
        let inner = Arc::clone(&self.inner.read());
        inner.validate()
    }
}

#[derive(Clone)]
struct Inner<K, V, D> {
    root: Option<Arc<Node<K, V, D>>>,
    seqno: u64,
    n_count: usize,
    n_deleted: usize,
}

enum Ir<K, V, D> {
    Root {
        inner: Inner<K, V, D>,
        old: Option<db::Entry<K, V, D>>,
    },
    Res {
        root: Option<Arc<Node<K, V, D>>>,
        old: Option<Arc<db::Entry<K, V, D>>>,
    },
}

impl<K, V, D> Ir<K, V, D> {
    #[allow(clippy::type_complexity)]
    fn into_root(self) -> (Inner<K, V, D>, Option<db::Entry<K, V, D>>) {
        match self {
            Ir::Root { inner, old } => (inner, old),
            _ => unreachable!(),
        }
    }

    #[allow(clippy::type_complexity)]
    fn into_res(self) -> (Option<Arc<Node<K, V, D>>>, Option<Arc<db::Entry<K, V, D>>>) {
        match self {
            Ir::Res { root, old } => (root, old),
            _ => unreachable!(),
        }
    }
}

impl<K, V, D> Inner<K, V, D> {
    fn set(&self, op: (K, V, Option<u64>, Option<u64>)) -> Result<Ir<K, V, D>>
    where
        K: Clone + Ord,
        V: Clone,
        D: Clone,
    {
        let (key, value, cas, seqno) = op;
        let seqno = seqno.unwrap_or_else(|| self.seqno.saturating_add(1));
        let root = self.root.as_ref().map(Borrow::borrow);

        let op = (key, value, cas, seqno);
        let (mut root, old) = self.do_set(root, op)?.into_res();

        root.as_mut()
            .map(|root| Arc::get_mut(root).map(Node::set_black));

        let n_count = compute_n_count!(self.n_count, old.as_ref());
        let n_deleted = compute_n_deleted!(self.n_deleted, old.as_ref());

        let inner = Inner {
            root,
            seqno: cmp::max(self.seqno, seqno),
            n_count,
            n_deleted,
        };

        let old = old.as_ref().map(|old| old.as_ref().clone());

        Ok(Ir::Root { inner, old })
    }

    fn insert(&self, op: (K, V, Option<u64>, Option<u64>)) -> Result<Ir<K, V, D>>
    where
        K: Clone + Ord,
        V: Clone + Diff<Delta = D>,
        D: Clone,
    {
        let (key, value, cas, seqno) = op;
        let seqno = seqno.unwrap_or_else(|| self.seqno.saturating_add(1));
        let root = self.root.as_ref().map(Borrow::borrow);

        let op = (key, value, cas, seqno);
        let (mut root, old) = self.do_insert(root, op)?.into_res();

        root.as_mut()
            .map(|root| Arc::get_mut(root).map(Node::set_black));

        let n_count = compute_n_count!(self.n_count, old.as_ref());
        let n_deleted = compute_n_deleted!(self.n_deleted, old.as_ref());

        let inner = Inner {
            root,
            seqno: cmp::max(self.seqno, seqno),
            n_count,
            n_deleted,
        };

        let old = old.as_ref().map(|old| old.as_ref().clone());

        Ok(Ir::Root { inner, old })
    }

    fn remove<Q>(&self, op: (&Q, Option<u64>, Option<u64>)) -> Result<Ir<K, V, D>>
    where
        K: Clone + Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
        V: Clone,
        D: Clone,
    {
        let (key, cas, seqno) = op;
        let seqno = seqno.unwrap_or_else(|| self.seqno.saturating_add(1));
        let root = self.root.as_ref().map(Borrow::borrow);

        let (mut root, old) = self.do_remove(root, (key, cas, seqno))?.into_res();

        root.as_mut()
            .map(|root| Arc::get_mut(root).map(Node::set_black));

        let (seqno, n_count) = if old.is_some() {
            (seqno, self.n_count - 1)
        } else {
            (self.seqno, self.n_count)
        };

        let inner = Inner {
            root,
            seqno: cmp::max(self.seqno, seqno),
            n_count,
            n_deleted: self.n_deleted,
        };

        let old = old.as_ref().map(|old| old.as_ref().clone());

        Ok(Ir::Root { inner, old })
    }

    fn delete<Q>(&self, op: (&Q, Option<u64>, Option<u64>)) -> Result<Ir<K, V, D>>
    where
        K: Clone + Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
        V: Clone + Diff<Delta = D>,
        D: Clone,
        <V as Diff>::Delta: From<V>,
    {
        let (key, cas, seqno) = op;
        let seqno = seqno.unwrap_or_else(|| self.seqno.saturating_add(1));
        let root = self.root.as_ref().map(Borrow::borrow);

        let (mut root, old) = self.do_delete(root, (key, cas, seqno))?.into_res();

        root.as_mut()
            .map(|root| Arc::get_mut(root).map(Node::set_black));

        let (n_count, n_deleted) = match old.as_ref() {
            Some(old) if !old.is_deleted() => (self.n_count, self.n_deleted + 1),
            Some(_) => (self.n_count, self.n_deleted),
            None => (self.n_count + 1, self.n_deleted + 1),
        };

        let inner = Inner {
            root,
            seqno: cmp::max(self.seqno, seqno),
            n_count,
            n_deleted,
        };

        let old = old.as_ref().map(|old| old.as_ref().clone());

        Ok(Ir::Root { inner, old })
    }

    fn commit<I>(&self, iter: I) -> Result<(Ir<K, V, D>, usize)>
    where
        K: Clone + PartialEq + Ord,
        V: Clone + Diff<Delta = D>,
        D: Clone + From<V>,
        I: Iterator<Item = db::Entry<K, V, D>>,
    {
        let mut n_entries = 0;
        let mut commit_seqno = self.seqno;
        let mut root = self.root.as_ref().map(|root| Arc::clone(root));
        let mut n_count = self.n_count;
        let mut n_deleted = self.n_deleted;

        for entry in iter {
            if entry.to_seqno() <= self.seqno {
                err_at!(
                    Invalid,
                    msg: "commit entry {} older than index snapshot {}",
                    entry.to_seqno(),
                    self.seqno
                )?;
            }
            n_entries += 1;

            commit_seqno = cmp::max(entry.to_seqno(), commit_seqno);
            root = {
                let is_deleted = entry.is_deleted();
                let (root, old) = self
                    .do_commit(root.as_ref().map(Borrow::borrow), entry)?
                    .into_res();
                match (is_deleted, old) {
                    (true, Some(old)) if !old.is_deleted() => {
                        n_deleted = n_deleted.saturating_add(1)
                    }
                    (false, Some(old)) if old.is_deleted() => {
                        n_deleted = n_deleted.saturating_sub(1)
                    }
                    (true, None) => {
                        n_deleted = n_deleted.saturating_add(1);
                        n_count = n_count.saturating_add(1);
                    }
                    (_, None) => {
                        n_count = n_count.saturating_add(1);
                    }
                    _ => (),
                };
                root
            };
        }

        let inner = Inner {
            root,
            seqno: commit_seqno,
            n_count,
            n_deleted,
        };

        Ok((Ir::Root { inner, old: None }, n_entries))
    }

    fn do_set(
        &self,
        node: Option<&Node<K, V, D>>,
        op: (K, V, Option<u64>, u64),
    ) -> Result<Ir<K, V, D>>
    where
        K: Clone + Ord,
        V: Clone,
        D: Clone,
    {
        let mut node: Node<K, V, D> = match (node, op.2) {
            (Some(node), _) => node.clone(),
            (None, Some(0)) | (None, None) => {
                let (key, value, _, seqno) = op;
                let node = db::Entry::new(key, value, seqno).into();
                let (root, old) = (Some(Arc::new(node)), None);
                return Ok(Ir::Res { root, old });
            }
            (None, Some(_)) => err_at!(InvalidCAS, msg: "invalid cas for missing set")?,
        };

        let cas = op.2;
        let (node, old) = match node.as_key().cmp(&op.0) {
            Ordering::Greater => {
                let left = node.left.as_ref().map(Borrow::borrow);
                let (root, old) = self.do_set(left, op)?.into_res();
                node.left = root;
                (walkuprot_23(node), old)
            }
            Ordering::Less => {
                let right = node.right.as_ref().map(Borrow::borrow);
                let (root, old) = self.do_set(right, op)?.into_res();
                node.right = root;
                (walkuprot_23(node), old)
            }
            Ordering::Equal if cas.is_none() || cas.unwrap() == node.to_seqno() => {
                let (_, value, _, seqno) = op;
                let old = node.entry.clone();
                node.set(value, seqno);
                (node, Some(old))
            }
            Ordering::Equal => {
                err_at!(InvalidCAS, msg: "{:?} != {}", cas, node.to_seqno())?
            }
        };
        let root = Some(Arc::new(node));

        Ok(Ir::Res { root, old })
    }

    fn do_insert(
        &self,
        node: Option<&Node<K, V, D>>,
        op: (K, V, Option<u64>, u64),
    ) -> Result<Ir<K, V, D>>
    where
        K: Clone + Ord,
        V: Clone + Diff<Delta = D>,
        D: Clone,
    {
        let mut node: Node<K, V, D> = match (node, op.2) {
            (Some(node), _) => node.clone(),
            (None, Some(0)) | (None, None) => {
                let (key, value, _, seqno) = op;
                let node = db::Entry::new(key, value, seqno).into();
                let (root, old) = (Some(Arc::new(node)), None);
                return Ok(Ir::Res { root, old });
            }
            (None, Some(_)) => err_at!(InvalidCAS, msg: "invalid cas for missing set")?,
        };

        let cas = op.2;
        let (node, old) = match node.as_key().cmp(&op.0) {
            Ordering::Greater => {
                let left = node.left.as_ref().map(Borrow::borrow);
                let (root, old) = self.do_insert(left, op)?.into_res();
                node.left = root;
                (walkuprot_23(node), old)
            }
            Ordering::Less => {
                let right = node.right.as_ref().map(Borrow::borrow);
                let (root, old) = self.do_insert(right, op)?.into_res();
                node.right = root;
                (walkuprot_23(node), old)
            }
            Ordering::Equal if cas.is_none() || cas.unwrap() == node.to_seqno() => {
                let (_, value, _, seqno) = op;
                let old = node.entry.clone();
                node.insert(value, seqno);
                (node, Some(old))
            }
            Ordering::Equal => {
                err_at!(InvalidCAS, msg: "{:?} != {}", cas, node.to_seqno())?
            }
        };
        let root = Some(Arc::new(node));

        Ok(Ir::Res { root, old })
    }

    fn do_remove<Q>(
        &self,
        node: Option<&Node<K, V, D>>,
        op: (&Q, Option<u64>, u64),
    ) -> Result<Ir<K, V, D>>
    where
        K: Clone + Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
        V: Clone,
        D: Clone,
    {
        let (key, cas, _) = op;

        let mut node: Node<K, V, D> = match (node, cas) {
            (Some(node), _) => node.clone(),
            (None, Some(0)) | (None, None) => {
                let (root, old) = (None, None);
                return Ok(Ir::Res { root, old });
            }
            (None, Some(_)) => err_at!(InvalidCAS, msg: "invalid cas for missing set")?,
        };

        let (root, old) = match node.as_key().borrow().cmp(key) {
            Ordering::Greater if node.left.is_none() => (Some(node), None),
            Ordering::Greater => {
                let left = node.as_left_ref();
                if !is_red(left) && !is_red(left.unwrap().as_left_ref()) {
                    node = move_red_left(node)
                }

                let (left, old) = self.do_remove(node.as_left_ref(), op)?.into_res();
                node.left = left;
                (Some(fixup(node)), old)
            }
            _ => {
                if is_red(node.as_left_ref()) {
                    node = rotate_right(node);
                }

                let cas_ok = cas.is_none() || cas.unwrap() == node.to_seqno();

                if node.as_key().borrow().eq(key) && !cas_ok {
                    err_at!(InvalidCAS, msg: "{} != {:?}", node.to_seqno(), cas)?;
                }

                if !node.as_key().borrow().lt(key) && node.right.is_none() {
                    (None, Some(node.entry.clone()))
                } else {
                    node = match node.as_right_ref() {
                        r @ Some(_)
                            if !is_red(r) && !is_red(r.unwrap().as_left_ref()) =>
                        {
                            move_red_right(node)
                        }
                        Some(_) | None => node,
                    };

                    if !node.as_key().borrow().lt(key) {
                        let [right, sub_node] = self.do_remove_min(node.as_right_ref());
                        node.right = right.map(Arc::new);
                        let mut sub_node = match sub_node {
                            Some(sub_node) => sub_node,
                            None => return err_at!(Fatal, msg: "call the programmer"),
                        };
                        sub_node.left = node.left;
                        sub_node.right = node.right;
                        sub_node.black = node.black;
                        (Some(fixup(sub_node)), Some(node.entry.clone()))
                    } else {
                        let right = node.as_right_ref();
                        let (right, old) = self.do_remove(right, op)?.into_res();
                        node.right = right;
                        (Some(fixup(node)), old)
                    }
                }
            }
        };
        let root = root.map(Arc::new);

        Ok(Ir::Res { root, old })
    }

    fn do_delete<Q>(
        &self,
        node: Option<&Node<K, V, D>>,
        op: (&Q, Option<u64>, u64),
    ) -> Result<Ir<K, V, D>>
    where
        K: Clone + Borrow<Q>,
        Q: Ord + ToOwned<Owned = K> + ?Sized,
        V: Clone + Diff<Delta = D>,
        D: Clone,
        <V as Diff>::Delta: From<V>,
    {
        let (key, cas, seqno) = op;

        let mut node: Node<K, V, D> = match (node, cas) {
            (Some(node), _) => node.clone(),
            (None, Some(0)) | (None, None) => {
                let key: K = key.to_owned();
                let (node, old) = (db::Entry::new_deleted(key, seqno).into(), None);
                let root = Some(Arc::new(node));
                return Ok(Ir::Res { root, old });
            }
            (None, Some(_)) => err_at!(InvalidCAS, msg: "invalid cas for missing set")?,
        };

        let cas_ok = cas.is_none() || cas.unwrap() == node.to_seqno();

        let (root, old) = match node.as_key().borrow().cmp(key) {
            Ordering::Greater => {
                let left = node.as_left_ref();
                let (root, old) = self.do_delete(left, op)?.into_res();
                node.left = root;
                (walkuprot_23(node), old)
            }
            Ordering::Less => {
                let right = node.as_right_ref();
                let (root, old) = self.do_delete(right, op)?.into_res();
                node.right = root;
                (walkuprot_23(node), old)
            }
            Ordering::Equal if cas_ok => {
                let old = node.entry.clone();
                node.delete(seqno);
                (walkuprot_23(node), Some(old))
            }
            Ordering::Equal => {
                err_at!(InvalidCAS, msg: "{} != {:?}", node.to_seqno(), cas)?
            }
        };
        let root = Some(Arc::new(root));

        Ok(Ir::Res { root, old })
    }

    fn do_remove_min(&self, node: Option<&Node<K, V, D>>) -> [Option<Node<K, V, D>>; 2]
    where
        K: Clone,
        V: Clone,
        D: Clone,
    {
        let mut node = match node {
            Some(node) => node.clone(),
            None => return [None, None],
        };

        if node.left.is_none() {
            return [None, Some(node)];
        }

        let left = node.as_left_ref();

        if !is_red(left) && !is_red(left.unwrap().as_left_ref()) {
            node = move_red_left(node);
        }
        let [left, sub_node] = self.do_remove_min(node.as_left_ref());
        node.left = left.map(Arc::new);
        [Some(fixup(node)), sub_node]
    }

    fn do_commit(
        &self,
        node: Option<&Node<K, V, D>>,
        entry: db::Entry<K, V, D>,
    ) -> Result<Ir<K, V, D>>
    where
        K: Clone + PartialEq + Ord,
        V: Clone + Diff<Delta = D>,
        D: Clone + From<V>,
    {
        let mut node: Node<K, V, D> = match node {
            Some(node) => node.clone(),
            None => {
                let (root, old) = (Some(Arc::new(entry.into())), None);
                return Ok(Ir::Res { root, old });
            }
        };

        let (node, old) = match node.as_key().cmp(entry.as_key()) {
            Ordering::Greater => {
                let left = node.left.as_ref().map(Borrow::borrow);
                let (root, old) = self.do_commit(left, entry)?.into_res();
                node.left = root;
                (walkuprot_23(node), old)
            }
            Ordering::Less => {
                let right = node.right.as_ref().map(Borrow::borrow);
                let (root, old) = self.do_commit(right, entry)?.into_res();
                node.right = root;
                (walkuprot_23(node), old)
            }
            Ordering::Equal => {
                let old = node.entry.clone();
                node.commit(entry);
                (node, Some(old))
            }
        };
        let root = Some(Arc::new(node));

        Ok(Ir::Res { root, old })
    }
}

impl<K, V, D> Inner<K, V, D> {
    fn get<Q>(&self, key: &Q) -> Result<db::Entry<K, V, D>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord + ?Sized,
    {
        let root = self.root.as_ref().map(Borrow::borrow);
        get(root, key)
    }

    fn iter(&self) -> Iter<K, V, D> {
        let root = self.root.as_ref().map(Arc::clone);
        let mut paths = Vec::default();
        build_iter(IFlag::Left, root, &mut paths);

        Iter { paths, frwrd: true }
    }

    fn range<R, Q>(&self, range: R) -> Range<K, V, D, R, Q>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        let root = self.root.as_ref().map(Arc::clone);

        let mut paths = Vec::default();
        match range.start_bound() {
            Bound::Unbounded => build_iter(IFlag::Left, root, &mut paths),
            Bound::Included(low) => find_start(root, low, true, &mut paths),
            Bound::Excluded(low) => find_start(root, low, false, &mut paths),
        };
        let iter = Iter { paths, frwrd: true };

        Range {
            range,
            iter,
            fin: false,
            high: marker::PhantomData,
        }
    }

    fn reverse<R, Q>(&self, range: R) -> Reverse<K, V, D, R, Q>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        let root = self.root.as_ref().map(Arc::clone);

        let mut paths = Vec::default();
        match range.end_bound() {
            Bound::Unbounded => build_iter(IFlag::Right, root, &mut paths),
            Bound::Included(high) => find_end(root, high, true, &mut paths),
            Bound::Excluded(high) => find_end(root, high, false, &mut paths),
        };
        let iter = Iter {
            paths,
            frwrd: false,
        };

        Reverse {
            range,
            iter,
            fin: false,
            low: marker::PhantomData,
        }
    }

    fn validate(&self) -> Result<()>
    where
        K: Ord + fmt::Debug,
    {
        let root = self.root.as_ref().map(Borrow::borrow);
        let (red, depth) = (is_red(root), 0);

        if red {
            err_at!(Fatal, msg: "root node must be black")?;
        }

        let n_blacks = 0;
        let (_, n_deleted, n_count) = validate_tree(root, red, n_blacks, depth)?;
        if n_deleted != self.n_deleted {
            err_at!(Fatal, msg: "n_deleted {} != {}", n_deleted, self.n_deleted)?;
        }
        if n_count != self.n_count {
            err_at!(Fatal, msg: "n_count {} != {}", n_count, self.n_count)?;
        }

        Ok(())
    }
}

#[inline]
fn is_red<K, V, D>(node: Option<&Node<K, V, D>>) -> bool {
    node.map_or(false, |node| !node.is_black())
}

#[inline]
fn is_black<K, V, D>(node: Option<&Node<K, V, D>>) -> bool {
    node.map_or(true, Node::is_black)
}

fn walkuprot_23<K, V, D>(mut node: Node<K, V, D>) -> Node<K, V, D>
where
    K: Clone,
    V: Clone,
    D: Clone,
{
    if is_red(node.as_right_ref()) && !is_red(node.as_left_ref()) {
        node = rotate_left(node)
    }
    let left = node.as_left_ref();
    if is_red(left) && is_red(left.unwrap().as_left_ref()) {
        node = rotate_right(node);
    }
    if is_red(node.as_left_ref()) && is_red(node.as_right_ref()) {
        flip(&mut node)
    }
    node
}

//              (i)                       (i)
//               |                         |
//              node                     right
//              /  \                      / \
//             /    (r)                 (r)  \
//            /       \                 /     \
//          left     right           node     r-r
//                    / \            /  \
//                 r-l  r-r       left  r-l
//
fn rotate_left<K, V, D>(mut node: Node<K, V, D>) -> Node<K, V, D>
where
    K: Clone,
    V: Clone,
    D: Clone,
{
    let old_right: &Node<K, V, D> = node.right.as_ref().map(|r| r.borrow()).unwrap();
    if is_black(Some(old_right)) {
        panic!("rotateleft(): rotate black link ? call-the-programmer");
    }

    let mut right = old_right.clone();

    node.right = right.left.take();
    right.black = node.black;
    node.set_red();
    right.left = Some(Arc::new(node));

    right
}

//              (i)                       (i)
//               |                         |
//              node                      left
//              /  \                      / \
//            (r)   \                   (r)  \
//           /       \                 /      \
//         left     right            l-l      node
//         / \                                / \
//      l-l  l-r                            l-r  right
//
fn rotate_right<K, V, D>(mut node: Node<K, V, D>) -> Node<K, V, D>
where
    K: Clone,
    V: Clone,
    D: Clone,
{
    let old_left: &Node<K, V, D> = node.left.as_ref().map(|l| l.borrow()).unwrap();
    if is_black(Some(old_left)) {
        panic!("rotateright(): rotate black link ? call-the-programmer")
    }

    let mut left = old_left.clone();

    node.left = left.right.take();
    left.black = node.black;
    node.set_red();
    left.right = Some(Arc::new(node));

    left
}

//        (x)                   (!x)
//         |                     |
//        node                  node
//        / \                   / \
//      (y) (z)              (!y) (!z)
//     /      \              /      \
//   left    right         left    right
//
fn flip<K, V, D>(node: &mut Node<K, V, D>)
where
    K: Clone,
    V: Clone,
    D: Clone,
{
    let mut left = {
        let left: &Node<K, V, D> = node.left.as_ref().map(|l| l.borrow()).unwrap();
        left.clone()
    };
    let mut right = {
        let right: &Node<K, V, D> = node.right.as_ref().map(|r| r.borrow()).unwrap();
        right.clone()
    };

    node.toggle_link();
    left.toggle_link();
    right.toggle_link();

    node.left = Some(Arc::new(left));
    node.right = Some(Arc::new(right));
}

fn fixup<K, V, D>(mut node: Node<K, V, D>) -> Node<K, V, D>
where
    K: Clone,
    V: Clone,
    D: Clone,
{
    if is_red(node.as_right_ref()) {
        node = rotate_left(node)
    }

    let left = node.as_left_ref();
    if is_red(left) && is_red(left.unwrap().as_left_ref()) {
        node = rotate_right(node)
    }

    if is_red(node.as_left_ref()) && is_red(node.as_right_ref()) {
        flip(&mut node)
    }
    node
}

fn move_red_left<K, V, D>(mut node: Node<K, V, D>) -> Node<K, V, D>
where
    K: Clone,
    V: Clone,
    D: Clone,
{
    flip(&mut node);

    if is_red(node.right.as_ref().unwrap().as_left_ref()) {
        let right = node.right.take().unwrap();
        let newr = {
            let rr: &Node<K, V, D> = right.borrow();
            rr.clone()
        };
        node.right = Some(Arc::new(rotate_right(newr)));
        node = rotate_left(node);
        flip(&mut node);
    }
    node
}

fn move_red_right<K, V, D>(mut node: Node<K, V, D>) -> Node<K, V, D>
where
    K: Clone,
    V: Clone,
    D: Clone,
{
    flip(&mut node);

    if is_red(node.left.as_ref().unwrap().as_left_ref()) {
        node = rotate_right(node);
        flip(&mut node);
    }
    node
}

// Get the latest version for key.
fn get<K, V, D, Q>(node: Option<&Node<K, V, D>>, key: &Q) -> Result<db::Entry<K, V, D>>
where
    K: Clone + Borrow<Q>,
    Q: Ord + ?Sized,
    V: Clone,
    D: Clone,
{
    match node {
        Some(nref) => match nref.as_key().borrow().cmp(key) {
            Ordering::Less => get(nref.as_right_ref(), key),
            Ordering::Greater => get(nref.as_left_ref(), key),
            Ordering::Equal => Ok(nref.entry.as_ref().clone()),
        },
        None => err_at!(KeyNotFound, msg: "get missing key"),
    }
}

fn validate_tree<K, V, D>(
    node: Option<&Node<K, V, D>>,
    fromred: bool,
    mut n_blacks: usize,
    depth: usize,
) -> Result<(usize, usize, usize)>
where
    K: Ord + fmt::Debug,
{
    let red = is_red(node);

    let node = match node {
        Some(_) if fromred && red => err_at!(Fatal, msg: "Mdb has consecutive reds")?,
        Some(node) => node,
        None => return Ok((n_blacks, 0, 0)),
    };

    if !red {
        n_blacks += 1;
    }

    if depth > MAX_TREE_DEPTH {
        err_at!(Fatal, msg: "tree exceeds max_depth {}", depth)?;
    }

    // confirm sort order in the tree.
    let (left, right) = {
        if let Some(left) = node.as_left_ref() {
            if left.as_key().ge(node.as_key()) {
                let (lk, nk) = (left.as_key(), node.as_key());
                err_at!(Fatal, msg: "Mdb left:{:?}, parent:{:?}", lk, nk)?;
            }
        }
        if let Some(right) = node.as_right_ref() {
            if right.as_key().le(node.as_key()) {
                let (rk, nk) = (right.as_key(), node.as_key());
                err_at!(Fatal, msg: "Mdb right:{:?}, parent:{:?}", rk, nk)?;
            }
        }
        (node.as_left_ref(), node.as_right_ref())
    };

    let (lb, ld, lc) = validate_tree(left, red, n_blacks, depth + 1)?;
    let (rb, rd, rc) = validate_tree(right, red, n_blacks, depth + 1)?;

    if lb != rb {
        err_at!(Fatal, msg: "Mdb unbalanced blacks l:{}, r:{}", lb, rb)?;
    }

    let mut n_deleted = ld + rd;
    n_deleted += if node.is_deleted() { 1 } else { 0 };

    Ok((lb, n_deleted, lc + rc + 1))
}

// Iterator type, to do full table scan.
//
// A full table scan using this type is optimal when used with concurrent
// read threads, but not with concurrent write threads.
pub struct Iter<K, V, D> {
    paths: Vec<Fragment<K, V, D>>,
    frwrd: bool,
}

impl<K, V, D> Iterator for Iter<K, V, D>
where
    K: Clone,
    V: Clone,
    D: Clone,
{
    type Item = db::Entry<K, V, D>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let path = self.paths.last_mut()?;
            if self.frwrd {
                match path.flag {
                    IFlag::Left => {
                        path.flag = IFlag::Center;
                        break Some(path.node.entry.as_ref().clone());
                    }
                    IFlag::Center => {
                        path.flag = IFlag::Right;
                        let right = path.node.right.as_ref().map(Arc::clone);
                        build_iter(IFlag::Left, right, &mut self.paths)
                    }
                    IFlag::Right => {
                        self.paths.pop();
                    }
                }
            } else {
                match path.flag {
                    IFlag::Right => {
                        path.flag = IFlag::Center;
                        break Some(path.node.entry.as_ref().clone());
                    }
                    IFlag::Center => {
                        path.flag = IFlag::Left;
                        let left = path.node.left.as_ref().map(Arc::clone);
                        build_iter(IFlag::Right, left, &mut self.paths)
                    }
                    IFlag::Left => {
                        self.paths.pop();
                    }
                }
            }
        }
    }
}

// Iterator type, to do range scan between a _lower-bound_ and _higher-bound_.
pub struct Range<K, V, D, R, Q>
where
    Q: ?Sized,
{
    range: R,
    iter: Iter<K, V, D>,
    fin: bool,
    high: marker::PhantomData<Q>,
}

impl<K, V, D, R, Q> Iterator for Range<K, V, D, R, Q>
where
    K: Clone + Borrow<Q>,
    V: Clone,
    D: Clone,
    Q: Ord + ?Sized,
    R: RangeBounds<Q>,
{
    type Item = db::Entry<K, V, D>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.fin {
            false => {
                let entry = self.iter.next()?;
                let qey = entry.as_key().borrow();
                match self.range.end_bound() {
                    Bound::Unbounded => Some(entry),
                    Bound::Included(high) if qey.le(high) => Some(entry),
                    Bound::Excluded(high) if qey.lt(high) => Some(entry),
                    Bound::Included(_) | Bound::Excluded(_) => {
                        self.fin = true;
                        None
                    }
                }
            }
            true => None,
        }
    }
}

// Iterator type, to do range scan between a _higher-bound_ and _lower-bound_.
pub struct Reverse<K, V, D, R, Q>
where
    Q: ?Sized,
{
    range: R,
    iter: Iter<K, V, D>,
    fin: bool,
    low: marker::PhantomData<Q>,
}

impl<K, V, D, R, Q> Iterator for Reverse<K, V, D, R, Q>
where
    K: Clone + Borrow<Q>,
    V: Clone,
    D: Clone,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = db::Entry<K, V, D>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.fin {
            false => {
                let entry = self.iter.next()?;
                let qey = entry.as_key().borrow();
                match self.range.start_bound() {
                    Bound::Unbounded => Some(entry),
                    Bound::Included(low) if qey.ge(low) => Some(entry),
                    Bound::Excluded(low) if qey.gt(low) => Some(entry),
                    Bound::Included(_) | Bound::Excluded(_) => {
                        self.fin = true;
                        None
                    }
                }
            }
            true => None,
        }
    }
}

// Continuous iteration without walking through the whole tree from root.
// Achieved by maintaining a FIFO queue of tree-path to the previous
// iterated node. Each node in the FIFO queue is a tuple of mdb-node and
// its current state (IFlag), together this tuple is called as a Fragment.
struct Fragment<K, V, D> {
    flag: IFlag,
    node: Arc<Node<K, V, D>>,
}
#[derive(Copy, Clone)]
enum IFlag {
    Left,   // left path is iterated.
    Center, // current node is iterated.
    Right,  // right paths is being iterated.
}

fn build_iter<K, V, D>(
    flag: IFlag,
    node: Option<Arc<Node<K, V, D>>>,
    paths: &mut Vec<Fragment<K, V, D>>,
) {
    if let Some(node) = node {
        let item = Fragment {
            flag,
            node: Arc::clone(&node),
        };
        let node = match flag {
            IFlag::Left => node.left.as_ref().map(Arc::clone),
            IFlag::Right => node.right.as_ref().map(Arc::clone),
            IFlag::Center => unreachable!(),
        };
        paths.push(item);
        build_iter(flag, node, paths)
    }
}

fn find_start<K, V, D, Q>(
    node: Option<Arc<Node<K, V, D>>>,
    low: &Q,
    incl: bool,
    paths: &mut Vec<Fragment<K, V, D>>,
) where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
{
    if let Some(node) = node {
        let left = node.left.as_ref().map(Arc::clone);
        let right = node.right.as_ref().map(Arc::clone);

        let cmp = node.as_key().borrow().cmp(low);

        let flag = match cmp {
            Ordering::Less => IFlag::Right,
            Ordering::Equal if incl => IFlag::Left,
            Ordering::Equal => IFlag::Center,
            Ordering::Greater => IFlag::Left,
        };
        paths.push(Fragment { flag, node });

        match cmp {
            Ordering::Equal => (),
            Ordering::Less => find_start(right, low, incl, paths),
            Ordering::Greater => find_start(left, low, incl, paths),
        }
    }
}

fn find_end<K, V, D, Q>(
    node: Option<Arc<Node<K, V, D>>>,
    high: &Q,
    incl: bool,
    paths: &mut Vec<Fragment<K, V, D>>,
) where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
{
    if let Some(node) = node {
        let left = node.left.as_ref().map(Arc::clone);
        let right = node.right.as_ref().map(Arc::clone);

        let cmp = node.as_key().borrow().cmp(high);

        let flag = match cmp {
            Ordering::Less => IFlag::Right,
            Ordering::Equal if incl => IFlag::Right,
            Ordering::Equal => IFlag::Center,
            Ordering::Greater => IFlag::Left,
        };
        paths.push(Fragment { flag, node });

        match cmp {
            Ordering::Equal => (),
            Ordering::Less => find_end(right, high, incl, paths),
            Ordering::Greater => find_end(left, high, incl, paths),
        }
    }
}

#[cfg(test)]
#[path = "mdb_test.rs"]
mod mdb_test;
