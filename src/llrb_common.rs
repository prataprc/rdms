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
    node.map_or(true, |node| node.is_black())
}

/// Get the latest version for key.
fn get<K, V, Q>(mut node: Option<&Node<K, V>>, key: &Q) -> Option<Entry<K, V>>
where
    K: Clone + Ord + Borrow<Q>,
    V: Clone + Diff,
    Q: Ord + ?Sized,
{
    while let Some(nref) = node {
        node = match nref.key_ref().borrow().cmp(key) {
            Ordering::Less => nref.right_deref(),
            Ordering::Greater => nref.left_deref(),
            Ordering::Equal => return Some(nref.entry.clone()),
        };
    }
    None
}

fn validate_tree<K, V>(
    node: Option<&Node<K, V>>,
    fromred: bool,
    mut nb: usize,
    depth: usize,
    stats: &mut LlrbStats,
) -> Result<usize, Error>
where
    K: Ord + Clone + Debug,
    V: Clone + Diff,
{
    let red = is_red(node);
    match node {
        Some(node) if node.dirty => Err(Error::DirtyNode),
        Some(_node) if fromred && red => Err(Error::ConsecutiveReds),
        Some(node) => {
            if !red {
                nb += 1;
            }
            let (left, right) = (node.left_deref(), node.right_deref());
            let l = validate_tree(left, red, nb, depth + 1, stats)?;
            let r = validate_tree(right, red, nb, depth + 1, stats)?;
            if l != r {
                return Err(Error::UnbalancedBlacks(l, r));
            }
            if let Some(left) = left {
                if left.key_ref().ge(node.key_ref()) {
                    let left = format!("{:?}", left.key_ref());
                    let parent = format!("{:?}", node.key_ref());
                    return Err(Error::SortError(left, parent));
                }
            }
            if let Some(right) = right {
                if right.key_ref().le(node.key_ref()) {
                    let parent = format!("{:?}", node.key_ref());
                    let right = format!("{:?}", right.key_ref());
                    return Err(Error::SortError(parent, right));
                }
            }
            Ok(l)
        }
        None => {
            stats.sample_depth(depth);
            Ok(nb)
        }
    }
}

// by default dropping a node does not drop its children.
fn drop_tree<K, V>(mut node: Box<Node<K, V>>)
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    //println!("drop_tree - node {:p}", node);

    // left child shall be dropped after drop_tree() returns.
    node.left.take().map(|left| drop_tree(left));
    // right child shall be dropped after drop_tree() returns.
    node.right.take().map(|right| drop_tree(right));
}

#[allow(dead_code)]
pub struct Iter<'a, K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    arc: Arc<MvccRoot<K, V>>,
    paths: Option<Vec<Fragment<'a, K, V>>>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Ord + Clone,
    V: Clone + Diff,
{
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut paths = match self.paths.take() {
            Some(paths) => paths,
            None => return None,
        };
        match paths.pop() {
            None => None,
            Some(mut path) => match (path.flag, path.nref) {
                (IFlag::Left, nref) => {
                    path.flag = IFlag::Center;
                    paths.push(path);
                    self.paths = Some(paths);
                    Some(nref.entry.clone())
                }
                (IFlag::Center, nref) => {
                    path.flag = IFlag::Right;
                    paths.push(path);
                    let rnref = nref.right_deref();
                    self.paths = Some(build_iter(IFlag::Left, rnref, paths));
                    self.next()
                }
                (_, _) => {
                    self.paths = Some(paths);
                    self.next()
                }
            },
        }
    }
}

#[allow(dead_code)]
pub struct Range<'a, K, V, R, Q>
where
    K: Ord + Clone + Borrow<Q>,
    V: Clone + Diff,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    arc: Arc<MvccRoot<K, V>>,
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
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut paths = match self.paths.take() {
            Some(paths) => paths,
            None => return None,
        };

        let item = match paths.pop() {
            None => None,
            Some(mut path) => match (path.flag, path.nref) {
                (IFlag::Left, nref) => {
                    path.flag = IFlag::Center;
                    paths.push(path);
                    self.paths = Some(paths);
                    Some(nref.entry.clone())
                }
                (IFlag::Center, nref) => {
                    path.flag = IFlag::Right;
                    paths.push(path);
                    let rnref = nref.right_deref();
                    self.paths = Some(build_iter(IFlag::Left, rnref, paths));
                    self.next()
                }
                (_, _) => {
                    self.paths = Some(paths);
                    self.next()
                }
            },
        };
        match item {
            None => None,
            Some(entry) => {
                let qey = entry.key_ref().borrow();
                match self.range.end_bound() {
                    Bound::Included(high) if qey.le(high) => Some(entry),
                    Bound::Excluded(high) if qey.lt(high) => Some(entry),
                    Bound::Unbounded => Some(entry),
                    Bound::Included(_) | Bound::Excluded(_) => {
                        self.paths.take();
                        None
                    }
                }
            }
        }
    }
}

#[allow(dead_code)]
pub struct Reverse<'a, K, V, R, Q>
where
    K: Ord + Clone,
    V: Clone + Diff,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
{
    arc: Arc<MvccRoot<K, V>>,
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
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut paths = match self.paths.take() {
            Some(paths) => paths,
            None => return None,
        };

        let item = match paths.pop() {
            None => None,
            Some(mut path) => match (path.flag, path.nref) {
                (IFlag::Right, nref) => {
                    path.flag = IFlag::Center;
                    paths.push(path);
                    self.paths = Some(paths);
                    Some(nref.entry.clone())
                }
                (IFlag::Center, nref) => {
                    path.flag = IFlag::Left;
                    paths.push(path);
                    let rnref = nref.left_deref();
                    self.paths = Some(build_iter(IFlag::Right, rnref, paths));
                    self.next()
                }
                (_, _) => {
                    self.paths = Some(paths);
                    self.next()
                }
            },
        };
        match item {
            None => None,
            Some(entry) => {
                let qey = entry.key_ref().borrow();
                match self.range.start_bound() {
                    Bound::Included(low) if qey.ge(low) => Some(entry),
                    Bound::Excluded(low) if qey.gt(low) => Some(entry),
                    Bound::Unbounded => Some(entry),
                    Bound::Included(_) | Bound::Excluded(_) => {
                        self.paths.take();
                        None
                    }
                }
            }
        }
    }
}

#[derive(Copy, Clone)]
enum IFlag {
    Left,
    Center,
    Right,
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
                IFlag::Left => nref.left_deref(),
                IFlag::Right => nref.right_deref(),
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
            let cmp = nref.key_ref().borrow().cmp(low);
            let flag = match cmp {
                Ordering::Less => IFlag::Right,
                Ordering::Equal if incl => IFlag::Left,
                Ordering::Equal => IFlag::Center,
                Ordering::Greater => IFlag::Left,
            };
            paths.push(Fragment { flag, nref });
            match cmp {
                Ordering::Less => {
                    let nref = nref.right_deref();
                    find_start(nref, low, incl, paths)
                }
                Ordering::Equal => paths,
                Ordering::Greater => {
                    let nref = nref.left_deref();
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
            let cmp = nref.key_ref().borrow().cmp(high);
            let flag = match cmp {
                Ordering::Less => IFlag::Right,
                Ordering::Equal if incl => IFlag::Right,
                Ordering::Equal => IFlag::Center,
                Ordering::Greater => IFlag::Left,
            };
            paths.push(Fragment { flag, nref });
            match cmp {
                Ordering::Less => {
                    let nref = nref.right_deref();
                    find_end(nref, high, incl, paths)
                }
                Ordering::Equal => paths,
                Ordering::Greater => {
                    let nref = nref.left_deref();
                    find_end(nref, high, incl, paths)
                }
            }
        }
    }
}
