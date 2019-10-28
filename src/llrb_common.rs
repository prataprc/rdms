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
    mut blacks: usize,
    depth: usize,
    depths: &mut LlrbDepth,
) -> Result<usize>
where
    K: Ord + Clone + Debug,
    V: Clone + Diff,
{
    let red = is_red(node);
    match node {
        Some(node) if node.dirty => {
            // TODO: llrb has dirty node.
            Err(Error::ValidationFail("llrb has dirty node".to_string()))
        }
        Some(_node) if fromred && red => {
            // TODO: llrb has dirty node.
            Err(Error::ValidationFail(format!("llrb has consecutive reds")))
        }
        Some(node) => {
            // confirm sort order in the tree.
            let (left, right) = {
                let left = node.as_left_deref();
                let right = node.as_right_deref();
                if let Some(left) = left {
                    if left.as_key().ge(node.as_key()) {
                        /// Fatal case, index entries are not in sort-order.
                        return Err(Error::ValidationFail(format!(
                            "llrb sort error left:{:?} parent:{:?}",
                            left.as_key(),
                            node.as_key()
                        )));
                    }
                }
                if let Some(right) = right {
                    if right.as_key().le(node.as_key()) {
                        /// Fatal case, index entries are not in sort-order.
                        return Err(Error::ValidationFail(format!(
                            "llrb sort error right:{:?} parent:{:?}",
                            right.as_key(),
                            node.as_key()
                        )));
                    }
                }
                (left, right)
            };

            {
                if !red {
                    blacks += 1;
                }
                let l = validate_tree(left, red, blacks, depth + 1, depths)?;
                let r = validate_tree(right, red, blacks, depth + 1, depths)?;
                if l != r {
                    return Err(Error::ValidationFail(format!(
                        "llrb has unbalacked blacks l:{}, r:{}",
                        l, r
                    )));
                }
                Ok(l)
            }
        }
        None => {
            depths.sample(depth);
            Ok(blacks)
        }
    }
}

/// Full table scan type for both [Llrb] and [Mvcc] index.
///
/// A full table scan using this type is optimal when used with concurrent
/// read threads, but not with concurrent write threads.
///
/// [Llrb]: crate::llrb::Llrb
/// [Mvcc]: crate::mvcc::Mvcc
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
                None => {
                    break None;
                }
            };

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
                        break Some(Ok(nref.entry.clone()));
                    }
                    (IFlag::Center, nref) => {
                        self.paths = {
                            path.flag = IFlag::Right;
                            paths.push(path);
                            let rnref = nref.as_right_deref();
                            Some(build_iter(IFlag::Left, rnref, paths))
                        };
                    }
                    (_, _) => self.paths = Some(paths),
                },
            }
        }
    }
}

/// Piece-wise full table scan type for both [Llrb] and [Mvcc] index.
///
/// Unlike [Iter] type, a full table scan using this type is optimal
/// for both concurrent reads and writes.
///
/// [Llrb]: crate::llrb::Llrb
/// [Mvcc]: crate::mvcc::Mvcc
pub struct IterPWScan<'a, K, V>
where
    K: Ord + Clone,
    V: Clone + Diff + From<<V as Diff>::D>,
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
    V: Clone + Diff + From<<V as Diff>::D>,
{
    type Item = Result<ScanEntry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        // loop for a maximum of 1000 entries.
        let mut limit = 1000; // TODO: avoid magic constants
        let mut key: K = unsafe { mem::zeroed() };
        loop {
            let mut paths = match self.paths.take() {
                Some(paths) => paths,
                None => {
                    break None;
                }
            };
            limit -= 1;
            if limit < 0 {
                break Some(Ok(ScanEntry::Retry(key)));
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
                                key = nref.entry.to_key();
                            }
                        }
                    }
                    (IFlag::Center, nref) => {
                        self.paths = {
                            path.flag = IFlag::Right;
                            paths.push(path);
                            let rnref = nref.as_right_deref();
                            Some(build_iter(IFlag::Left, rnref, paths))
                        };
                    }
                    (_, _) => self.paths = Some(paths),
                },
            };
        }
    }
}

/// Range scan between a _`lower-bound`_ and _`higher-bound`_ for bot [`Llrb`]
/// and [Mvcc] index.
///
/// [Llrb]: crate::llrb::Llrb
/// [Mvcc]: crate::mvcc::Mvcc
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
                    break None;
                }
            };

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
                        break Some(nref.entry.clone());
                    }
                    (IFlag::Center, nref) => {
                        self.paths = {
                            path.flag = IFlag::Right;
                            paths.push(path);
                            let rnref = nref.as_right_deref();
                            Some(build_iter(IFlag::Left, rnref, paths))
                        };
                    }
                    (_, _) => self.paths = Some(paths),
                },
            };
        };

        match item {
            None => None,
            Some(entry) => {
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
        }
    }
}

/// Reverse range scan between a _`higher-bound`_ and _`lower-bound`_ for both
/// [`Llrb`] and [Mvcc] index.
///
/// [Llrb]: crate::llrb::Llrb
/// [Mvcc]: crate::mvcc::Mvcc
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
                    break None;
                }
            };

            match paths.pop() {
                None => {
                    break None;
                }
                Some(mut path) => match (path.flag, path.nref) {
                    (IFlag::Right, nref) => {
                        self.paths = {
                            path.flag = IFlag::Center;
                            paths.push(path);
                            Some(paths)
                        };
                        break Some(nref.entry.clone());
                    }
                    (IFlag::Center, nref) => {
                        self.paths = {
                            path.flag = IFlag::Left;
                            paths.push(path);
                            let rnref = nref.as_left_deref();
                            Some(build_iter(IFlag::Right, rnref, paths))
                        };
                    }
                    (_, _) => self.paths = Some(paths),
                },
            };
        };

        match item {
            None => None,
            Some(entry) => {
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
) -> Vec<Fragment<'a, K, V>>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    match nref {
        None => paths,
        Some(nref) => {
            let item = Fragment { flag, nref };
            let nref = match flag {
                IFlag::Left => nref.as_left_deref(),
                IFlag::Right => nref.as_right_deref(),
                IFlag::Center => unreachable!(),
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
