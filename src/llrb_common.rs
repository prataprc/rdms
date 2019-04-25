const ITER_LIMIT: usize = 100;

type Entry<K, V> = core::Entry<K, V>;

fn is_red<K, V>(node: Option<&Node<K, V>>) -> bool
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    node.map_or(false, |node| !node.is_black())
}

fn is_black<K, V>(node: Option<&Node<K, V>>) -> bool
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    node.map_or(true, |node| node.is_black())
}

fn get<K, V, Q>(
    mut node: Option<&Node<K, V>>, // root node
    key: &Q,
) -> Option<Entry<K, V>>
where
    K: Clone + Ord + Borrow<Q> + Debug,
    V: Default + Clone + Diff + Serialize,
    Q: Ord + ?Sized,
{
    while node.is_some() {
        let nref = node.unwrap();
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
    stats: &mut Stats,
) -> Result<usize, BognError>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    let red = is_red(node);
    match node {
        Some(node) if node.dirty => Err(BognError::DirtyNode),
        Some(_node) if fromred && red => Err(BognError::ConsecutiveReds),
        Some(node) => {
            if !red {
                nb += 1;
            }
            let (left, right) = (node.left_deref(), node.right_deref());
            let l = validate_tree(left, red, nb, depth + 1, stats)?;
            let r = validate_tree(right, red, nb, depth + 1, stats)?;
            if l != r {
                return Err(BognError::UnbalancedBlacks(l, r));
            }
            if let Some(left) = left {
                if left.key_ref().ge(node.key_ref()) {
                    let left = format!("{:?}", left.key_ref());
                    let parent = format!("{:?}", node.key_ref());
                    return Err(BognError::SortError(left, parent));
                }
            }
            if let Some(right) = right {
                if right.key_ref().le(node.key_ref()) {
                    let parent = format!("{:?}", node.key_ref());
                    let right = format!("{:?}", right.key_ref());
                    return Err(BognError::SortError(parent, right));
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

pub struct Iter<'a, K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    arc: Arc<MvccRoot<K, V>>,
    root: Option<&'a Node<K, V>>,
    iter: std::vec::IntoIter<Entry<K, V>>,
    after_key: Option<Bound<K>>,
    limit: usize,
}

impl<'a, K, V> Iter<'a, K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    fn get_root(&self) -> Option<&Node<K, V>> {
        match self.root {
            root @ Some(_) => root,
            None => self.arc.as_ref().root_ref(),
        }
    }

    fn batch_scan(
        &self,
        node: Option<&Node<K, V>>,
        acc: &mut Vec<Entry<K, V>>, // batch of entries
    ) -> bool {
        if node.is_none() {
            return true;
        }
        let node = node.unwrap();

        let (left, right) = (node.left_deref(), node.right_deref());
        match &self.after_key {
            None => return false,
            Some(Bound::Included(akey)) | Some(Bound::Excluded(akey)) => {
                if node.key_ref().borrow().le(akey) {
                    return self.batch_scan(right, acc);
                }
            }
            Some(Bound::Unbounded) => (),
        }

        if !self.batch_scan(left, acc) {
            return false;
        }

        acc.push(node.entry.clone());
        if acc.len() >= self.limit {
            return false;
        }

        return self.batch_scan(right, acc);
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => {
                let mut acc: Vec<Entry<K, V>> = Vec::with_capacity(self.limit);
                self.batch_scan(self.get_root(), &mut acc);
                self.after_key = acc.last().map(|x| Bound::Excluded(x.key()));
                self.iter = acc.into_iter();
                self.iter.next()
            }
            item @ Some(_) => item,
        }
    }
}

pub struct Range<'a, K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    arc: Arc<MvccRoot<K, V>>,
    root: Option<&'a Node<K, V>>,
    iter: std::vec::IntoIter<Entry<K, V>>,
    low: Option<Bound<K>>,
    high: Bound<K>,
    limit: usize,
}

impl<'a, K, V> Range<'a, K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    fn get_root(&self) -> Option<&Node<K, V>> {
        match self.root {
            root @ Some(_) => root,
            None => self.arc.as_ref().root_ref(), // Arc<MvccRoot>
        }
    }

    pub fn rev(self) -> Reverse<'a, K, V> {
        Reverse {
            arc: self.arc,
            root: self.root,
            iter: vec![].into_iter(),
            high: Some(self.high),
            low: self.low.unwrap(),
            limit: self.limit,
        }
    }

    fn batch_scan(
        &self,
        node: Option<&Node<K, V>>,
        acc: &mut Vec<Entry<K, V>>, // batch of entries
    ) -> bool {
        if node.is_none() {
            return true;
        }
        let node = node.unwrap();

        let (left, right) = (node.left_deref(), node.right_deref());
        match &self.low {
            Some(Bound::Included(qow)) if node.key_ref().lt(qow) => {
                return self.batch_scan(right, acc);
            }
            Some(Bound::Excluded(qow)) if node.key_ref().le(qow) => {
                return self.batch_scan(right, acc);
            }
            _ => (),
        }

        if !self.batch_scan(left, acc) {
            return false;
        }

        acc.push(node.entry.clone());
        if acc.len() >= self.limit {
            return false;
        }

        self.batch_scan(right, acc)
    }
}

impl<'a, K, V> Iterator for Range<'a, K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = match self.iter.next() {
            None if self.low.is_some() => {
                let mut acc: Vec<Entry<K, V>> = Vec::with_capacity(self.limit);
                self.batch_scan(self.get_root(), &mut acc);
                self.low = acc.last().map(|x| Bound::Excluded(x.key()));
                self.iter = acc.into_iter();
                self.iter.next()
            }
            None => None,
            item @ Some(_) => item,
        };
        // check for lower bound
        match item {
            None => None,
            Some(item) => match &self.high {
                Bound::Unbounded => Some(item),
                Bound::Included(qigh) if item.key_ref().le(qigh) => Some(item),
                Bound::Excluded(qigh) if item.key_ref().lt(qigh) => Some(item),
                _ => {
                    self.low = None;
                    None
                }
            },
        }
    }
}

pub struct Reverse<'a, K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    arc: Arc<MvccRoot<K, V>>,
    root: Option<&'a Node<K, V>>,
    iter: std::vec::IntoIter<Entry<K, V>>,
    high: Option<Bound<K>>,
    low: Bound<K>,
    limit: usize,
}

impl<'a, K, V> Reverse<'a, K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    fn get_root(&self) -> Option<&Node<K, V>> {
        match self.root {
            root @ Some(_) => root,
            None => self.arc.as_ref().root_ref(),
        }
    }

    fn batch_scan(
        &self,
        node: Option<&Node<K, V>>,
        acc: &mut Vec<Entry<K, V>>, // batch of entries
    ) -> bool {
        if node.is_none() {
            return true;
        }
        let node = node.unwrap();

        let (left, right) = (node.left_deref(), node.right_deref());
        match &self.high {
            Some(Bound::Included(qigh)) if node.key_ref().gt(qigh) => {
                return self.batch_scan(left, acc);
            }
            Some(Bound::Excluded(qigh)) if node.key_ref().ge(qigh) => {
                return self.batch_scan(left, acc);
            }
            _ => (),
        }

        if !self.batch_scan(right, acc) {
            return false;
        }

        acc.push(node.entry.clone());
        if acc.len() >= self.limit {
            return false;
        }

        return self.batch_scan(left, acc);
    }
}

impl<'a, K, V> Iterator for Reverse<'a, K, V>
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = match self.iter.next() {
            None if self.high.is_some() => {
                let mut acc: Vec<Entry<K, V>> = Vec::with_capacity(self.limit);
                self.batch_scan(self.get_root(), &mut acc);
                self.high = acc.last().map(|x| Bound::Excluded(x.key()));
                self.iter = acc.into_iter();
                self.iter.next()
            }
            None => None,
            item @ Some(_) => item,
        };
        // check for lower bound
        match item {
            None => None,
            Some(item) => match &self.low {
                Bound::Unbounded => Some(item),
                Bound::Included(qow) if item.key_ref().ge(qow) => Some(item),
                Bound::Excluded(qow) if item.key_ref().gt(qow) => Some(item),
                _ => {
                    self.high = None;
                    None
                }
            },
        }
    }
}

fn drop_tree<K, V>(mut node: Box<Node<K, V>>)
where
    K: Clone + Ord + Debug,
    V: Default + Clone + Diff + Serialize,
{
    //println!("drop_tree - node {:p}", node);
    node.left.take().map(|left| drop_tree(left));
    node.right.take().map(|right| drop_tree(right));
}
