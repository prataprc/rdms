use std::{convert::TryInto, fmt, ops::Deref, result};

use crate::core::{Diff, Entry, Footprint, Result, ToJson, Value};
use crate::spinlock;

#[allow(unused_imports)] // for documentation
use crate::llrb::Llrb;

/// Node corresponds to a single entry in Llrb instance.
#[derive(Clone)]
pub(crate) struct Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub(crate) entry: Entry<K, V>,
    pub(crate) black: bool,                    // store: black or red
    pub(crate) dirty: bool,                    // new node in mvcc path
    pub(crate) left: Option<Box<Node<K, V>>>,  // store: left child
    pub(crate) right: Option<Box<Node<K, V>>>, // store: right child
}

impl<K, V> Footprint for Node<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    fn footprint(&self) -> Result<isize> {
        use std::mem::size_of;

        let size = size_of::<Node<K, V>>() - size_of::<Entry<K, V>>();
        let overhead: isize = size.try_into().unwrap();
        Ok(overhead + self.entry.footprint()?)
    }
}

// construct node values.
impl<K, V> Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    // lsm delete.
    pub(crate) fn new_deleted(key: K, deleted: u64) -> Box<Node<K, V>> {
        let node = Box::new(Node {
            entry: Entry::new(key, Value::new_delete(deleted)),
            black: false,
            dirty: true,
            left: None,
            right: None,
        });
        //println!("new node {:p}", node);
        node
    }

    // unsafe clone for MVCC CoW
    pub(crate) fn mvcc_clone(
        &self,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
        copyval: bool,
    ) -> Box<Node<K, V>> {
        let new_node = Box::new(Node {
            entry: self.entry.mvcc_clone(copyval),
            black: self.black,
            dirty: self.dirty,
            left: self.left.as_ref().map(|n| n.duplicate()),
            right: self.right.as_ref().map(|n| n.duplicate()),
        });
        //println!("new node (mvcc) {:p} {:p}", self, new_node);
        reclaim.push(self.duplicate());
        new_node
    }

    // remove this node from the tree without dropping the children.
    pub(crate) fn mvcc_detach(&mut self) {
        self.left.take().map(Box::leak);
        self.right.take().map(Box::leak);
    }

    // clone this node without cloning its children. dirty is set to true.
    pub(crate) fn clone_detach(&self) -> Node<K, V> {
        Node {
            entry: self.entry.clone(),
            black: self.black,
            dirty: true,
            left: None,
            right: None,
        }
    }

    #[inline]
    pub(crate) fn duplicate(&self) -> Box<Node<K, V>> {
        unsafe { Box::from_raw(self as *const Node<K, V> as *mut Node<K, V>) }
    }
}

// write/update methods
impl<K, V> Node<K, V>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
{
    // prepend operation, equivalent to SET / INSERT / UPDATE
    // return the different in footprint for this node
    pub(crate) fn prepend_version(
        &mut self,
        entry: Entry<K, V>,
        lsm: bool, /* will preseve old mutations*/
    ) -> Result<isize> {
        self.entry.prepend_version(entry, lsm)
    }

    // DELETE operation, back to back delete shall collapse
    #[inline]
    pub(crate) fn delete(&mut self, seqno: u64) -> Result<isize> {
        self.entry.delete(seqno)
    }

    #[inline]
    pub(crate) fn set_red(&mut self) {
        self.black = false
    }

    #[inline]
    pub(crate) fn set_black(&mut self) {
        self.black = true
    }

    #[inline]
    pub(crate) fn toggle_link(&mut self) {
        self.black = !self.black
    }
}

// read methods
impl<K, V> Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    #[inline]
    pub(crate) fn as_left_deref(&self) -> Option<&Node<K, V>> {
        self.left.as_ref().map(Deref::deref)
    }

    #[inline]
    pub(crate) fn as_right_deref(&self) -> Option<&Node<K, V>> {
        self.right.as_ref().map(Deref::deref)
    }

    #[inline]
    pub(crate) fn is_black(&self) -> bool {
        self.black
    }

    pub(crate) fn as_key(&self) -> &K {
        self.entry.as_key()
    }

    pub(crate) fn to_seqno(&self) -> u64 {
        self.entry.to_seqno()
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.entry.is_deleted()
    }
}

// fence recursive drops
impl<K, V> Drop for Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn drop(&mut self) {
        self.left.take().map(Box::leak);
        self.right.take().map(Box::leak);
    }
}

impl<K, V> From<Entry<K, V>> for Node<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    fn from(entry: Entry<K, V>) -> Node<K, V> {
        Node {
            entry,
            black: false,
            dirty: true,
            left: None,
            right: None,
        }
    }
}

/// Statistics for [`Llrb`] and [`Mvcc`] tree.
pub enum Stats {
    /// full statisics via [`Llrb::validate`] method.
    Llrb {
        name: String,
        entries: usize,
        node_size: usize,
        rw_latch: spinlock::Stats,
        blacks: Option<usize>,
        depths: Option<LlrbDepth>,
    },
    /// partial but quick statistics via [`Llrb::stats`] method.
    Mvcc {
        name: String,
        entries: usize,
        node_size: usize,
        rw_latch: spinlock::Stats,
        snapshot_latch: spinlock::Stats,
        blacks: Option<usize>,
        depths: Option<LlrbDepth>,
    },
}

impl Stats {
    pub(crate) fn new_llrb_partial(
        name: &str,
        entries: usize,
        node_size: usize,
        rw_latch: spinlock::Stats,
    ) -> Stats {
        Stats::Llrb {
            name: name.to_string(),
            entries,
            node_size,
            rw_latch,
            blacks: None,
            depths: None,
        }
    }

    pub(crate) fn new_llrb_full(
        name: &str,
        entries: usize,
        node_size: usize,
        rw_latch: spinlock::Stats,
        blacks: usize,
        depths: LlrbDepth,
    ) -> Stats {
        Stats::Llrb {
            name: name.to_string(),
            entries,
            node_size,
            rw_latch,
            blacks: Some(blacks),
            depths: Some(depths),
        }
    }

    pub(crate) fn new_mvcc_partial(
        name: &str,
        entries: usize,
        node_size: usize,
        rw_latch: spinlock::Stats,
        snapshot_latch: spinlock::Stats,
    ) -> Stats {
        Stats::Mvcc {
            name: name.to_string(),
            entries,
            node_size,
            rw_latch,
            snapshot_latch,
            blacks: None,
            depths: None,
        }
    }

    pub(crate) fn new_mvcc_full(
        name: &str,
        entries: usize,
        node_size: usize,
        rw_latch: spinlock::Stats,
        snapshot_latch: spinlock::Stats,
        blacks: usize,
        depths: LlrbDepth,
    ) -> Stats {
        Stats::Mvcc {
            name: name.to_string(),
            entries,
            node_size,
            rw_latch,
            snapshot_latch,
            blacks: Some(blacks),
            depths: Some(depths),
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let none = "none".to_string();
        match self {
            Stats::Llrb {
                name,
                entries,
                node_size,
                rw_latch,
                blacks,
                depths,
            } => {
                let b = blacks.as_ref().map_or(none.clone(), |x| x.to_string());
                let d = depths.as_ref().map_or(none.clone(), |x| x.to_string());
                write!(f, r#"llrb.name = {}\n"#, name);
                write!(
                    f,
                    r#"llrb = {{ entries = {}, node_size = {}, blacks = {} }}"#,
                    entries, node_size, b,
                );
                write!(f, "llrb.rw_latch = {}\n", rw_latch);
                write!(f, "llrb.depths = {}\n", d);
            }
            Stats::Mvcc {
                name,
                entries,
                node_size,
                rw_latch,
                snapshot_latch,
                blacks,
                depths,
            } => {
                let b = blacks.as_ref().map_or(none.clone(), |x| x.to_string());
                let d = depths.as_ref().map_or(none.clone(), |x| x.to_string());
                write!(f, r#"mvcc.name = {}\n"#, name);
                write!(
                    f,
                    r#"mvcc = {{ entries = {}, node_size = {}, blacks = {} }}"#,
                    entries, node_size, b,
                );
                write!(f, "mvcc.rw_latch = {}\n", rw_latch);
                write!(f, "mvcc.snap_latch = {}\n", snapshot_latch);
                write!(f, "mvcc.depths = {}\n", d);
            }
        }
        Ok(())
    }
}

impl ToJson for Stats {
    fn to_json(&self) -> String {
        let nil = "nil".to_string();
        match self {
            Stats::Llrb {
                name,
                entries,
                node_size,
                rw_latch,
                blacks,
                depths,
            } => {
                let l_stats = rw_latch.to_json();
                format!(
                    concat!(
                        r#"{{ ""llrb": {{ "name": {}, "entries": {:X}, ",
                        r#""node_size": {}, "#,
                        r#""rw_latch": {}, "blacks": {}, "depths": {} }} }}"#,
                    ),
                    name,
                    entries,
                    node_size,
                    l_stats,
                    blacks.as_ref().map_or(nil.clone(), |x| format!("{}", x)),
                    depths.as_ref().map_or(nil.clone(), |x| x.to_json()),
                )
            }
            Stats::Mvcc {
                name,
                entries,
                node_size,
                rw_latch,
                snapshot_latch,
                blacks,
                depths,
            } => {
                let rw_l = rw_latch.to_json();
                let snap_l = snapshot_latch.to_json();
                format!(
                    concat!(
                        r#"{{ ""mvcc": {{ "name": {}, "entries": {:X}, ",
                        r#""node_size": {}, "rw_latch": {}, "#,
                        r#""snap_latch": {}, "blacks": {}, "depths": {} }} }}"#,
                    ),
                    name,
                    entries,
                    node_size,
                    rw_l,
                    snap_l,
                    blacks.as_ref().map_or(nil.clone(), |x| format!("{}", x)),
                    depths.as_ref().map_or(nil.clone(), |x| x.to_json()),
                )
            }
        }
    }
}

// TODO: test cases for Depth.

/// LlrbDepth calculates minimum, maximum, average and percentile of
/// leaf-node depth in the LLRB tree.
#[derive(Clone)]
pub struct LlrbDepth {
    samples: usize,
    min: usize,
    max: usize,
    total: usize,
    depths: [u64; 256],
}

impl LlrbDepth {
    pub(crate) fn sample(&mut self, depth: usize) {
        self.samples += 1;
        self.total += depth;
        self.min = usize::min(self.min, depth);
        self.max = usize::max(self.max, depth);
        self.depths[depth] += 1;
    }

    /// Return number of leaf-nodes sample for depth in LLRB tree.
    pub fn to_samples(&self) -> usize {
        self.samples
    }

    /// Return minimum depth of leaf-node in LLRB tree.
    pub fn to_min(&self) -> usize {
        self.min
    }

    /// Return the average depth of leaf-nodes in LLRB tree.
    pub fn to_mean(&self) -> usize {
        self.total / self.samples
    }

    /// Return maximum depth of leaf-node in LLRB tree.
    pub fn to_max(&self) -> usize {
        self.max
    }

    /// Return depth as tuple of percentiles, each tuple provides
    /// (percentile, depth). Returned percentiles from 91 .. 99
    pub fn to_percentiles(&self) -> Vec<(u8, usize)> {
        let mut percentiles: Vec<(u8, usize)> = vec![];
        let (mut acc, mut prev_perc) = (0_u64, 90_u8);
        let iter = self.depths.iter().enumerate().filter(|(_, &item)| item > 0);
        for (depth, samples) in iter {
            acc += *samples;
            let perc = ((acc as f64 / (self.samples as f64)) * 100_f64) as u8;
            if perc > prev_perc {
                percentiles.push((perc, depth));
                prev_perc = perc;
            }
        }
        percentiles
    }
}

impl fmt::Display for LlrbDepth {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let (m, n, x) = (self.to_min(), self.to_mean(), self.to_max());
        let props: Vec<String> = self
            .to_percentiles()
            .into_iter()
            .map(|(perc, depth)| format!(r#""{}" = {}"#, perc, depth))
            .collect();
        let depth = props.join(", ");

        write!(f, "{{ samples = {}, ", self.samples)?;
        write!(f, "min = {}, mean = {}, max = {}, ", m, n, x)?;
        write!(f, "percentiles = {{ {} }} }}", depth)
    }
}

impl ToJson for LlrbDepth {
    fn to_json(&self) -> String {
        let props: Vec<String> = self
            .to_percentiles()
            .into_iter()
            .map(|(d, n)| format!(r#""{}": {}"#, d, n))
            .collect();
        let strs = [
            format!(r#""samples": {}"#, self.to_samples()),
            format!(r#""min": {}"#, self.to_min()),
            format!(r#""mean": {}"#, self.to_mean()),
            format!(r#""max": {}"#, self.to_max()),
            format!(r#""percentiles": {{ {} }}"#, props.join(", ")),
        ];
        format!(r#"{{ {} }}"#, strs.join(", "))
    }
}

impl Default for LlrbDepth {
    fn default() -> Self {
        LlrbDepth {
            samples: 0,
            min: std::usize::MAX,
            max: std::usize::MIN,
            total: 0,
            depths: [0; 256],
        }
    }
}
