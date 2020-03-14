const MAX_TREE_DEPTH: usize = 100;

pub(crate) struct SquashDebris<K, V>
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    pub(crate) root: Option<Box<Node<K, V>>>,
    pub(crate) seqno: u64,
    pub(crate) n_count: usize,
    pub(crate) n_deleted: usize,
    pub(crate) key_footprint: isize,
    pub(crate) tree_footprint: isize,
}

#[inline]
fn is_red<K, V>(node: Option<&Node<K, V>>) -> bool
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    node.map_or(false, |node| !node.is_black())
}

#[inline]
fn is_black<K, V>(node: Option<&Node<K, V>>) -> bool
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    node.map_or(true, Node::is_black)
}

fn do_shards<K, V>(nref: Option<&Node<K, V>>, shards: usize, acc: &mut Vec<Option<K>>)
where
    K: Clone + Ord,
    V: Clone + Diff,
{
    match nref {
        None => acc.push(None),
        Some(_) if shards == 0 => (),
        Some(nref) if shards == 1 => acc.push(Some(nref.to_key())),
        Some(nref) if shards == 2 => {
            do_shards(nref.as_left_deref(), 1, acc);
            do_shards(nref.as_right_deref(), 1, acc);
        }
        Some(nref) => {
            let lhalf = shards - (shards / 2);
            do_shards(nref.as_left_deref(), lhalf, acc);
            do_shards(nref.as_right_deref(), shards - lhalf, acc);
        }
    }
}

/// Get the latest version for key.
fn get<K, V, Q>(node: Option<&Node<K, V>>, key: &Q) -> Result<Entry<K, V>>
where
    K: Clone + Ord + Borrow<Q>,
    V: Clone + Diff,
    Q: Ord + ?Sized,
{
    match node {
        Some(nref) => match nref.as_key().borrow().cmp(key) {
            Ordering::Less => get(nref.as_right_deref(), key),
            Ordering::Greater => get(nref.as_left_deref(), key),
            Ordering::Equal => Ok(nref.entry.clone()),
        },
        None => Err(Error::KeyNotFound),
    }
}

// list of validation done by this function
// * Verify the sort order between a node and its left/right child.
// * No node which has RIGHT RED child and LEFT BLACK child (or NULL child).
// * Make sure there are no dirty nodes.
// * Make sure there are no consecutive reds.
// * Make sure number of blacks are same on both left and right arm.
fn validate_tree<K, V>(
    node: Option<&Node<K, V>>,
    fromred: bool,
    mut ss: (usize, usize),
    depth: usize,
    depths: &mut LlrbDepth,
) -> Result<(usize, usize)>
where
    K: Ord + Clone + fmt::Debug,
    V: Clone + Diff,
{
    let red = is_red(node);
    let node = match node {
        Some(node) if node.dirty => {
            let msg = "llrb has dirty node".to_string();
            return Err(Error::ValidationFail(msg));
        }
        Some(_node) if fromred && red => {
            let msg = "llrb has consecutive reds".to_string();
            return Err(Error::ValidationFail(msg));
        }
        Some(node) => node,
        None => {
            depths.sample(depth);
            return Ok(ss);
        }
    };

    // confirm sort order in the tree.
    let (left, right) = {
        if let Some(left) = node.as_left_deref() {
            if left.as_key().ge(node.as_key()) {
                /// Fatal case, index entries are not in sort-order.
                return Err(Error::ValidationFail(format!(
                    "llrb sort error left:{:?} parent:{:?}",
                    left.as_key(),
                    node.as_key()
                )));
            }
        }
        if let Some(right) = node.as_right_deref() {
            if right.as_key().le(node.as_key()) {
                /// Fatal case, index entries are not in sort-order.
                return Err(Error::ValidationFail(format!(
                    "llrb sort error right:{:?} parent:{:?}",
                    right.as_key(),
                    node.as_key()
                )));
            }
        }
        (node.as_left_deref(), node.as_right_deref())
    };

    if !red {
        ss.0 += 1;
    }
    let mut ss_l = validate_tree(left, red, ss.clone(), depth + 1, depths)?;
    let ss_r = validate_tree(right, red, ss.clone(), depth + 1, depths)?;

    {
        if ss_l.0 != ss_r.0 {
            return Err(Error::ValidationFail(format!(
                "llrb has unbalacked blacks l:{}, r:{}",
                ss_l.0, ss_r.0
            )));
        }
    }
    {
        ss_l.1 = if node.is_deleted() {
            1 + ss_l.1 + ss_r.1
        } else {
            ss_l.1 + ss_r.1
        };
    }

    Ok(ss_l)
}

/// Iterator type, to do full table scan, for both [Llrb] and [Mvcc] index.
///
/// A full table scan using this type is optimal when used with concurrent
/// read threads, but not with concurrent write threads.
pub struct Iter<'a, K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    _latch: Option<spinlock::Reader<'a>>, // only used for latching
    _arc: Arc<Snapshot<K, V>>,            // only used for MVCC-snapshot refcount.
    paths: Option<Vec<Fragment<'a, K, V>>>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut paths = match self.paths.take() {
                Some(paths) => paths,
                None => break None,
            };

            match paths.pop() {
                None => break None,
                Some(mut path) => match (path.flag, path.nref) {
                    (IFlag::Left, nref) => {
                        self.paths = {
                            path.flag = IFlag::Center;
                            paths.push(path);
                            Some(paths)
                        };
                        break Some(Ok(nref.entry.clone()));
                    }
                    (IFlag::Center, nref) => {
                        self.paths = {
                            path.flag = IFlag::Right;
                            paths.push(path);
                            let rnref = nref.as_right_deref();
                            match build_iter(IFlag::Left, rnref, paths) {
                                Ok(paths) => Some(paths),
                                Err(err) => break Some(Err(err)),
                            }
                        };
                    }
                    (_, _) => self.paths = Some(paths),
                },
            }
        }
    }
}

/// Iterator type, to do piece-wise full table scan, for both [Llrb] and
/// [Mvcc] index.
pub struct IterPWScan<'a, K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    _latch: Option<spinlock::Reader<'a>>,
    _arc: Arc<Snapshot<K, V>>, // only used for ref-count-ing MVCC-snapshot.
    start: Bound<u64>,
    end: Bound<u64>,
    paths: Option<Vec<Fragment<'a, K, V>>>,
}

impl<'a, K, V> Iterator for IterPWScan<'a, K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    type Item = Result<ScanEntry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        // loop for a maximum of 1000 entries.
        let mut limit = 1000; // TODO: avoid magic constants
        let mut key: Option<K> = None;
        loop {
            let mut paths = match self.paths.take() {
                Some(paths) => paths,
                None => {
                    break None;
                }
            };
            limit -= 1;
            if limit < 0 {
                break Some(Ok(ScanEntry::Retry(key.unwrap())));
            }

            match paths.pop() {
                None => {
                    break None;
                }
                Some(mut path) => match (path.flag, path.nref) {
                    (IFlag::Left, nref) => {
                        self.paths = {
                            path.flag = IFlag::Center;
                            paths.push(path);
                            Some(paths)
                        };
                        // include if entry was within the visible time-range
                        let (a, z) = (self.start.clone(), self.end.clone());
                        // {
                        //     let seqno = nref.entry.to_seqno();
                        //     println!("{:?} {:?} {}", a, z, seqno);
                        // }
                        match nref.entry.filter_within(a, z) {
                            Some(entry) => break Some(Ok(ScanEntry::Found(entry))),
                            None => {
                                key = Some(nref.entry.to_key());
                            }
                        }
                    }
                    (IFlag::Center, nref) => {
                        self.paths = {
                            path.flag = IFlag::Right;
                            paths.push(path);
                            let rnref = nref.as_right_deref();
                            match build_iter(IFlag::Left, rnref, paths) {
                                Ok(paths) => Some(paths),
                                Err(err) => break Some(Err(err)),
                            }
                        };
                    }
                    (_, _) => self.paths = Some(paths),
                },
            };
        }
    }
}

/// Iterator type, to do range scan between a _lower-bound_ and
/// _higher-bound_, for both [`Llrb`] and [Mvcc] index.
pub struct Range<'a, K, V, R, Q>
where
    K: Ord + Clone + Borrow<Q>,
    V: Clone + Diff,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    _latch: Option<spinlock::Reader<'a>>, // only used for latching
    _arc: Arc<Snapshot<K, V>>,            // only used for MVCC-snapshot refcount.
    range: R,
    paths: Option<Vec<Fragment<'a, K, V>>>,
    high: marker::PhantomData<Q>,
}

impl<'a, K, V, R, Q> Iterator for Range<'a, K, V, R, Q>
where
    K: Ord + Clone + Borrow<Q>,
    V: Clone + Diff,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = loop {
            let mut paths = match self.paths.take() {
                Some(paths) => paths,
                None => {
                    break Ok(None);
                }
            };

            match paths.pop() {
                None => {
                    break Ok(None);
                }
                Some(mut path) => match (path.flag, path.nref) {
                    (IFlag::Left, nref) => {
                        self.paths = {
                            path.flag = IFlag::Center;
                            paths.push(path);
                            Some(paths)
                        };
                        break Ok(Some(nref.entry.clone()));
                    }
                    (IFlag::Center, nref) => {
                        self.paths = {
                            path.flag = IFlag::Right;
                            paths.push(path);
                            let rnref = nref.as_right_deref();
                            match build_iter(IFlag::Left, rnref, paths) {
                                Ok(paths) => Some(paths),
                                Err(err) => break Err(err),
                            }
                        };
                    }
                    (_, _) => self.paths = Some(paths),
                },
            };
        };

        match item {
            Ok(None) => None,
            Ok(Some(entry)) => {
                let qey = entry.as_key().borrow();
                match self.range.end_bound() {
                    Bound::Unbounded => Some(Ok(entry)),
                    Bound::Included(high) if qey.le(high) => Some(Ok(entry)),
                    Bound::Excluded(high) if qey.lt(high) => Some(Ok(entry)),
                    Bound::Included(_) | Bound::Excluded(_) => {
                        self.paths.take();
                        None
                    }
                }
            }
            Err(err) => Some(Err(err)),
        }
    }
}

/// Iterator type, to do range scan between a _higher-bound_ and
/// _lower-bound_ for both [`Llrb`] and [Mvcc] index.
pub struct Reverse<'a, K, V, R, Q>
where
    K: Ord + Clone,
    V: Clone + Diff,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    _latch: Option<spinlock::Reader<'a>>, // only used for latching
    _arc: Arc<Snapshot<K, V>>,            // only used for MVCC-snapshot refcount.
    range: R,
    paths: Option<Vec<Fragment<'a, K, V>>>,
    low: marker::PhantomData<Q>,
}

impl<'a, K, V, R, Q> Iterator for Reverse<'a, K, V, R, Q>
where
    K: Ord + Clone + Borrow<Q>,
    V: Clone + Diff,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = loop {
            let mut paths = match self.paths.take() {
                Some(paths) => paths,
                None => {
                    break Ok(None);
                }
            };

            match paths.pop() {
                None => {
                    break Ok(None);
                }
                Some(mut path) => match (path.flag, path.nref) {
                    (IFlag::Right, nref) => {
                        self.paths = {
                            path.flag = IFlag::Center;
                            paths.push(path);
                            Some(paths)
                        };
                        break Ok(Some(nref.entry.clone()));
                    }
                    (IFlag::Center, nref) => {
                        self.paths = {
                            path.flag = IFlag::Left;
                            paths.push(path);
                            let rnref = nref.as_left_deref();
                            match build_iter(IFlag::Right, rnref, paths) {
                                Ok(paths) => Some(paths),
                                Err(err) => break Err(err),
                            }
                        };
                    }
                    (_, _) => self.paths = Some(paths),
                },
            };
        };

        match item {
            Ok(None) => None,
            Ok(Some(entry)) => {
                let qey = entry.as_key().borrow();
                match self.range.start_bound() {
                    Bound::Included(low) if qey.ge(low) => Some(Ok(entry)),
                    Bound::Excluded(low) if qey.gt(low) => Some(Ok(entry)),
                    Bound::Unbounded => Some(Ok(entry)),
                    Bound::Included(_) | Bound::Excluded(_) => {
                        self.paths.take();
                        None
                    }
                }
            }
            Err(err) => Some(Err(err)),
        }
    }
}

// We support continuous iteration without walking through the whole
// tree from root. We do this by maintaining a FIFO queue of tree-path
// to the previous iterated node. Each node in the FIFO queue is a tuple
// of llrb-node and its current state (IFlag), together this tuple is
// called as a Fragment.

#[derive(Copy, Clone)]
enum IFlag {
    Left,   // left path is iterated.
    Center, // current node is iterated.
    Right,  // right paths is being iterated.
}

struct Fragment<'a, K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    flag: IFlag,
    nref: &'a Node<K, V>,
}

fn build_iter<'a, K, V>(
    flag: IFlag,
    nref: Option<&'a Node<K, V>>, // subtree
    mut paths: Vec<Fragment<'a, K, V>>,
) -> Result<Vec<Fragment<'a, K, V>>>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    match nref {
        None => Ok(paths),
        Some(nref) => {
            let item = Fragment { flag, nref };
            let nref = match flag {
                IFlag::Left => nref.as_left_deref(),
                IFlag::Right => nref.as_right_deref(),
                IFlag::Center => err_at!(Fatal, msg: format!("unreachable"))?,
            };
            paths.push(item);
            build_iter(flag, nref, paths)
        }
    }
}

fn find_start<'a, K, V, Q>(
    nref: Option<&'a Node<K, V>>,
    low: &Q,
    incl: bool,
    mut paths: Vec<Fragment<'a, K, V>>,
) -> Vec<Fragment<'a, K, V>>
where
    K: Ord + Clone + Borrow<Q>,
    V: Clone + Diff,
    Q: Ord + ?Sized,
{
    match nref {
        None => paths,
        Some(nref) => {
            let cmp = nref.as_key().borrow().cmp(low);
            paths.push(Fragment {
                flag: match cmp {
                    Ordering::Less => IFlag::Right,
                    Ordering::Equal if incl => IFlag::Left,
                    Ordering::Equal => IFlag::Center,
                    Ordering::Greater => IFlag::Left,
                },
                nref,
            });
            match cmp {
                Ordering::Less => {
                    let nref = nref.as_right_deref();
                    find_start(nref, low, incl, paths)
                }
                Ordering::Equal => paths,
                Ordering::Greater => {
                    let nref = nref.as_left_deref();
                    find_start(nref, low, incl, paths)
                }
            }
        }
    }
}

fn find_end<'a, K, V, Q>(
    nref: Option<&'a Node<K, V>>,
    high: &Q,
    incl: bool,
    mut paths: Vec<Fragment<'a, K, V>>,
) -> Vec<Fragment<'a, K, V>>
where
    K: Ord + Clone + Borrow<Q>,
    V: Clone + Diff,
    Q: Ord + ?Sized,
{
    match nref {
        None => paths,
        Some(nref) => {
            let cmp = nref.as_key().borrow().cmp(high);
            paths.push(Fragment {
                flag: match cmp {
                    Ordering::Less => IFlag::Right,
                    Ordering::Equal if incl => IFlag::Right,
                    Ordering::Equal => IFlag::Center,
                    Ordering::Greater => IFlag::Left,
                },
                nref,
            });
            match cmp {
                Ordering::Less => {
                    let nref = nref.as_right_deref();
                    find_end(nref, high, incl, paths)
                }
                Ordering::Equal => paths,
                Ordering::Greater => {
                    let nref = nref.as_left_deref();
                    find_end(nref, high, incl, paths)
                }
            }
        }
    }
}
