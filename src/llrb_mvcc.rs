#[derive(Default)]
struct MvccRoot<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    root: ManuallyDrop<Option<Box<Node<K, V>>>>,
    reclaim: Vec<Box<Node<K, V>>>,
    seqno: u64,   // starts from 0 and incr for every mutation.
    n_count: u64, // number of entries in the tree.
    next: Option<Arc<MvccRoot<K, V>>>,
}

impl<K, V> MvccRoot<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn as_ref(&self) -> Option<&Node<K, V>> {
        self.root.as_ref().map(|item| item.deref())
    }
}

impl<K, V> Clone for MvccRoot<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn clone(&self) -> MvccRoot<K, V> {
        let mut new = MvccRoot {
            root: ManuallyDrop::new(None),
            seqno: self.seqno,
            n_count: self.n_count,
            reclaim: Default::default(),
            next: Some(Arc::new(Default::default())),
        };

        if self.root.is_some() {
            new.root = self.root.clone()
        }

        new
    }
}

struct Mvcc<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<K, V> Mvcc<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn set(
        llrb: &Llrb<K, V>, /* main index */
        key: K,
        value: V,
    ) -> Option<impl AsEntry<K, V>> {
        let lsm = llrb.lsm;
        let mvcc_root = llrb.snapshot.as_mut();
        let seqno = mvcc_root.seqno + 1;
        let mut n_count = mvcc_root.n_count;
        let root = mvcc_root.root.as_mut().map(|item| item.deref_mut());
        // TODO: no magic number
        let mut reclaim: Vec<Box<Node<K, V>>> = Vec::with_capacity(128);

        match Mvcc::upsert(root, key, value, seqno, lsm, &mut reclaim) {
            (Some(mut root), old_node) => {
                root.set_black();
                if old_node.is_none() {
                    n_count += 1;
                }
                llrb.snapshot
                    .move_next_snapshot(Some(root), seqno, n_count, reclaim);
                old_node
            }
            (None, _old_node) => unreachable!(),
        }
    }

    fn upsert(
        node: Option<&mut Node<K, V>>,
        key: K,
        value: V,
        seqno: u64,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>) {
        if node.is_none() {
            let black = false;
            return (Some(Box::new(Node::new(key, value, seqno, black))), None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        //node = Mvcc::walkdown_rot23(node);

        let cmp = new_node.key.cmp(&key);
        if cmp == Ordering::Greater {
            let left = new_node.left_deref_mut();
            let (l, o) = Mvcc::upsert(left, key, value, seqno, lsm, reclaim);
            new_node.left = l;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), o)
        } else if cmp == Ordering::Less {
            let right = new_node.right_deref_mut();
            let (r, o) = Mvcc::upsert(right, key, value, seqno, lsm, reclaim);
            new_node.right = r;
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), o)
        } else {
            let old_node = node.clone_detach();
            new_node.prepend_version(value, seqno, lsm);
            (Some(Mvcc::walkuprot_23(new_node, reclaim)), Some(old_node))
        }
    }

    fn set_cas(
        llrb: &Llrb<K, V>,
        key: K,
        value: V,
        cas: u64,
    ) -> Result<Option<impl AsEntry<K, V>>, BognError> {
        let lsm = llrb.lsm;
        let mvcc_root = llrb.snapshot.as_mut();
        let seqno = mvcc_root.seqno + 1;
        let mut n_count = mvcc_root.n_count;
        let root = mvcc_root.root.as_mut().map(|item| item.deref_mut());
        // TODO: no magic number
        let mut reclaim: Vec<Box<Node<K, V>>> = Vec::with_capacity(128);

        match Mvcc::upsert_cas(root, key, value, cas, seqno, lsm, &mut reclaim) {
            (_, _, Some(err)) => Err(err),
            (Some(mut root), old_node, None) => {
                root.set_black();
                if old_node.is_none() {
                    n_count += 1
                }
                llrb.snapshot
                    .move_next_snapshot(Some(root), seqno, n_count, reclaim);
                Ok(old_node)
            }
            _ => panic!("set_cas: impossible case, call programmer"),
        }
    }

    fn upsert_cas(
        node: Option<&mut Node<K, V>>,
        key: K,
        val: V,
        cas: u64,
        seqno: u64,
        lsm: bool,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (
        Option<Box<Node<K, V>>>,
        Option<Node<K, V>>,
        Option<BognError>,
    ) {
        if node.is_none() && cas > 0 {
            return (None, None, Some(BognError::InvalidCAS));
        } else if node.is_none() {
            let black = false;
            let node = Box::new(Node::new(key, val, seqno, black));
            return (Some(node), None, None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        // node = Mvcc::walkdown_rot23(node);

        let (r, cmp) = (reclaim, new_node.key.cmp(&key));
        let (old_node, err) = if cmp == Ordering::Greater {
            let (k, v, left) = (key, val, new_node.left_deref_mut());
            let (l, o, e) = Mvcc::upsert_cas(left, k, v, cas, seqno, lsm, r);
            new_node.left = l;
            (o, e)
        } else if cmp == Ordering::Less {
            let (k, v, right) = (key, val, new_node.right_deref_mut());
            let (rh, o, e) = Mvcc::upsert_cas(right, k, v, cas, seqno, lsm, r);
            new_node.right = rh;
            (o, e)
        } else if new_node.is_deleted() && cas != 0 && cas != new_node.seqno() {
            (None, Some(BognError::InvalidCAS))
        } else if !new_node.is_deleted() && cas != new_node.seqno() {
            (None, Some(BognError::InvalidCAS))
        } else {
            let old_node = node.clone_detach();
            new_node.prepend_version(val, seqno, lsm);
            (Some(old_node), None)
        };

        return (Some(Mvcc::walkuprot_23(new_node, r)), old_node, err);
    }

    fn delete<Q>(llrb: &mut Llrb<K, V>, key: &Q) -> Option<impl AsEntry<K, V>>
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        let mvcc_root = llrb.snapshot.as_mut();
        let mut seqno = mvcc_root.seqno + 1;
        let mut n_count = mvcc_root.n_count;
        let root = mvcc_root.root.as_mut().map(|item| item.deref_mut());
        // TODO: no magic number
        let mut reclaim: Vec<Box<Node<K, V>>> = Vec::with_capacity(128);

        let (root, old_node) = if llrb.lsm {
            let (root, oldn) = Mvcc::delete_lsm(root, key, seqno, &mut reclaim);
            let mut root = root.unwrap();
            root.set_black();
            if oldn.is_none() {
                n_count += 1
            } else if oldn.as_ref().unwrap().is_deleted() {
                seqno -= 1
            }
            (Some(root), oldn)
        } else {
            // in non-lsm mode remove the entry from the tree.
            let (root, oldn) = match Mvcc::do_delete(root, key, &mut reclaim) {
                (None, oldn) => (None, oldn),
                (Some(mut root), oldn) => {
                    root.set_black();
                    (Some(root), oldn)
                }
            };
            if oldn.is_some() {
                n_count -= 1;
            } else {
                seqno -= 1
            }
            (root, oldn.map(|item| *item))
        };
        llrb.snapshot
            .move_next_snapshot(root, seqno, n_count, reclaim);
        old_node
    }

    fn delete_lsm<Q>(
        node: Option<&mut Node<K, V>>,
        key: &Q,
        seqno: u64,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>)
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if node.is_none() {
            let (key, black) = (key.clone().into(), false);
            let mut node = Node::new(key, Default::default(), seqno, black);
            node.delete(seqno, true /*lsm*/);
            return (Some(Box::new(node)), None);
        }

        let node = node.unwrap();
        let mut new_node = node.mvcc_clone(reclaim);
        //let mut node = Mvcc::walkdown_rot23(node.unwrap());

        let old_node = match new_node.key.borrow().cmp(&key) {
            Ordering::Greater => {
                let left = new_node.left_deref_mut();
                let (l, old_node) = Mvcc::delete_lsm(left, key, seqno, reclaim);
                new_node.left = l;
                old_node
            }
            Ordering::Less => {
                let right = new_node.right_deref_mut();
                let (r, old_node) = Mvcc::delete_lsm(right, key, seqno, reclaim);
                new_node.right = r;
                old_node
            }
            Ordering::Equal => {
                new_node.delete(seqno, true /*lsm*/);
                Some(node.clone_detach())
            }
        };

        (Some(Mvcc::walkuprot_23(new_node, reclaim)), old_node)
    }

    // this is the non-lsm path.
    fn do_delete<Q>(
        node: Option<&mut Node<K, V>>,
        key: &Q,
        reclaim: &mut Vec<Box<Node<K, V>>>,
    ) -> (Option<Box<Node<K, V>>>, Option<Box<Node<K, V>>>)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let node = match node {
            None => return (None, None),
            Some(node) => node,
        };

        let mut new_node = node.mvcc_clone(reclaim);

        if new_node.key.borrow().gt(key) {
            if new_node.left.is_none() {
                // key not present, nothing to delete
                (Some(new_node), None)
            } else {
                let ok = !is_red(new_node.left_deref());
                if ok && !is_red(new_node.left.as_ref().unwrap().left_deref()) {
                    new_node = Mvcc::move_red_left(new_node, reclaim);
                }
                let left = new_node.left_deref_mut();
                let (left, old_node) = Mvcc::do_delete(left, key, reclaim);
                new_node.left = left;
                (Some(Mvcc::fixup(new_node, reclaim)), old_node)
            }
        } else {
            if is_red(new_node.left_deref()) {
                new_node = Mvcc::rotate_right(new_node, reclaim);
            }

            // if key equals node and no right children
            if !new_node.key.borrow().lt(key) && new_node.right.is_none() {
                new_node.mvcc_detach();
                return (None, Some(new_node));
            }

            let ok = new_node.right.is_some() && !is_red(new_node.right_deref());
            if ok && !is_red(new_node.right.as_ref().unwrap().left_deref()) {
                new_node = Mvcc::move_red_right(new_node, reclaim);
            }

            // if key equal node and there is a right children
            if !new_node.key.borrow().lt(key) {
                // node == key
                let right = new_node.right_deref_mut();
                let (right, mut res_node) = Mvcc::delete_min(right, reclaim);
                new_node.right = right;
                if res_node.is_none() {
                    panic!("do_delete(): fatal logic, call the programmer");
                }
                let mut newnode = res_node.take().unwrap();
                newnode.left = new_node.left.take();
                newnode.right = new_node.right.take();
                newnode.black = new_node.black;
                (Some(Mvcc::fixup(newnode, reclaim)), Some(new_node))
            } else {
                let right = new_node.right_deref_mut();
                let (right, old_node) = Mvcc::do_delete(right, key, reclaim);
                new_node.right = right;
                (Some(Mvcc::fixup(new_node, reclaim)), old_node)
            }
        }
    }

    // return [node, old_node]
    fn delete_min(
        node: Option<&mut Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> (Option<Box<Node<K, V>>>, Option<Box<Node<K, V>>>) {
        if node.is_none() {
            return (None, None);
        }

        let mut new_node = node.unwrap().mvcc_clone(reclaim);

        if new_node.left.is_none() {
            new_node.mvcc_detach();
            return (None, Some(new_node));
        }
        let left = new_node.left_deref();
        if !is_red(left) && !is_red(left.unwrap().left_deref()) {
            new_node = Mvcc::move_red_left(new_node, reclaim);
        }
        let left = new_node.left_deref_mut();
        let (left, old_node) = Mvcc::delete_min(left, reclaim);
        new_node.left = left;
        (Some(Mvcc::fixup(new_node, reclaim)), old_node)
    }

    ////--------- rotation routines for 2-3 algorithm ----------------

    fn walkdown_rot23(node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node
    }

    fn walkuprot_23(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        if is_red(node.right_deref()) && !is_red(node.left_deref()) {
            node = Mvcc::rotate_left(node, reclaim);
        }
        let left = node.left_deref();
        if is_red(left) && is_red(left.unwrap().left_deref()) {
            node = Mvcc::rotate_right(node, reclaim);
        }
        if is_red(node.left_deref()) && is_red(node.right_deref()) {
            Mvcc::flip(node.deref_mut(), reclaim)
        }
        node
    }

    //              (i)                       (i)
    //               |                         |
    //              node                       x
    //              /  \                      / \
    //             /    (r)                 (r)  \
    //            /       \                 /     \
    //          left       x             node      xr
    //                    / \            /  \
    //                  xl   xr       left   xl
    //
    fn rotate_left(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        let mut right = node.right_deref_mut().unwrap().mvcc_clone(reclaim);
        if is_black(Some(right.as_ref())) {
            panic!("rotateleft(): rotating a black link ? call the programmer");
        }
        node.right = right.left;
        right.black = node.black;
        node.set_red();
        right.left = Some(node);
        right
    }

    //              (i)                       (i)
    //               |                         |
    //              node                       x
    //              /  \                      / \
    //            (r)   \                   (r)  \
    //           /       \                 /      \
    //          x       right             xl      node
    //         / \                                / \
    //       xl   xr                             xr  right
    //
    fn rotate_right(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        let mut left = node.left_deref_mut().unwrap().mvcc_clone(reclaim);
        if is_black(Some(left.as_ref())) {
            panic!("rotateright(): rotating a black link ? call the programmer")
        }
        node.left = left.right;
        left.black = node.black;
        node.set_red();
        left.right = Some(node);
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
    fn flip(node: &mut Node<K, V>, reclaim: &mut Vec<Box<Node<K, V>>>) {
        let mut left = node.left_deref_mut().unwrap().mvcc_clone(reclaim);
        let mut right = node.right_deref_mut().unwrap().mvcc_clone(reclaim);

        left.toggle_link();
        right.toggle_link();
        node.toggle_link();

        node.left = Some(left);
        node.right = Some(right);
    }

    fn fixup(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        node = if is_red(node.right_deref()) {
            Mvcc::rotate_left(node, reclaim)
        } else {
            node
        };
        node = {
            let left = node.left_deref();
            if is_red(left) && is_red(left.unwrap().left_deref()) {
                Mvcc::rotate_right(node, reclaim)
            } else {
                node
            }
        };
        if is_red(node.left_deref()) && is_red(node.right_deref()) {
            Mvcc::flip(node.deref_mut(), reclaim);
        }
        node
    }

    fn move_red_left(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        Mvcc::flip(node.deref_mut(), reclaim);
        if is_red(node.right.as_ref().unwrap().left_deref()) {
            let right = node.right.take().unwrap();
            node.right = Some(Mvcc::rotate_right(right, reclaim));
            node = Mvcc::rotate_left(node, reclaim);
            Mvcc::flip(node.deref_mut(), reclaim);
        }
        node
    }

    fn move_red_right(
        mut node: Box<Node<K, V>>,
        reclaim: &mut Vec<Box<Node<K, V>>>, /* reclaim */
    ) -> Box<Node<K, V>> {
        Mvcc::flip(node.deref_mut(), reclaim);
        if is_red(node.left.as_ref().unwrap().left_deref()) {
            node = Mvcc::rotate_right(node, reclaim);
            Mvcc::flip(node.deref_mut(), reclaim);
        }
        node
    }
}

struct Snapshot<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    value: AtomicPtr<Arc<MvccRoot<K, V>>>,
}

impl<K, V> Snapshot<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn new() -> Snapshot<K, V> {
        let mut mvcc_root: MvccRoot<K, V> = Default::default();
        let arc_next = Arc::new(Default::default());
        mvcc_root.next = Some(arc_next);

        let mut arc = Arc::new(mvcc_root);
        let snapshot = Snapshot {
            value: AtomicPtr::new(&mut arc),
        };

        std::mem::forget(arc);
        snapshot
    }

    fn move_next_snapshot(
        &self,
        root: Option<Box<Node<K, V>>>,
        seqno: u64,
        n_count: u64,
        reclaim: Vec<Box<Node<K, V>>>,
    ) {
        use std::sync::atomic::Ordering::Relaxed;

        let arc = unsafe {
            Arc::from_raw(self.value.load(Relaxed)) // gets arc-dropped
        };
        let mut next_arc = Arc::clone(arc.as_ref().next.as_ref().unwrap());

        let mvcc_root = Arc::get_mut(&mut next_arc).unwrap();
        mvcc_root.root = ManuallyDrop::new(root);
        mvcc_root.seqno = seqno;
        mvcc_root.n_count = n_count;
        mvcc_root.reclaim = reclaim;
        mvcc_root.next = Some(Arc::new(Default::default()));
        self.value.store(&mut next_arc, Relaxed);
        std::mem::forget(arc);
    }

    fn clone(&self) -> Arc<MvccRoot<K, V>> {
        use std::sync::atomic::Ordering::Relaxed;

        Arc::clone(unsafe { self.value.load(Relaxed).as_ref().unwrap() })
    }

    fn root_leak(&self) -> Option<Box<Node<K, V>>> {
        use std::sync::atomic::Ordering::Relaxed;

        let arc = unsafe { self.value.load(Relaxed).as_mut().unwrap() };
        match Arc::get_mut(arc).unwrap().root.deref_mut() {
            Some(root) => unsafe { Some(Box::from_raw(root.deref_mut())) },
            None => None,
        }
    }

    fn as_mut(&self) -> &mut MvccRoot<K, V> {
        use std::sync::atomic::Ordering::Relaxed;

        let arc = unsafe { self.value.load(Relaxed).as_mut().unwrap() };
        Arc::get_mut(arc).unwrap()
    }
}

impl<K, V> AsRef<MvccRoot<K, V>> for Snapshot<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn as_ref(&self) -> &MvccRoot<K, V> {
        use std::sync::atomic::Ordering::Relaxed;

        unsafe { self.value.load(Relaxed).as_ref().unwrap() }
    }
}
