struct Single<K, V> {
    key: PhantomData<K>,
    value: PhantomData<V>,
}

type OBN<K, V> = Option<Box<Node<K, V>>>;

impl<K, V> Single<K, V>
where
    K: AsKey,
    V: Default + Clone,
{
    fn set(
        llrb: &mut Llrb<K, V>, /* main index */
        key: K,
        value: V,
    ) -> Option<impl AsEntry<K, V>> {
        let seqno = llrb.seqno + 1;
        let root = llrb.root.take();

        let old_node = match Single::upsert(root, key, value, seqno, llrb.lsm) {
            (Some(mut root), old_node) => {
                root.set_black();
                llrb.root = Some(root);
                old_node
            }
            (None, old_node) => old_node,
        };

        llrb.seqno = seqno;
        if old_node.is_none() {
            llrb.n_count += 1;
        }
        old_node
    }

    fn upsert(
        node: Option<Box<Node<K, V>>>,
        key: K,
        value: V,
        seqno: u64,
        lsm: bool,
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>) {
        if node.is_none() {
            let black = false;
            return (Some(Box::new(Node::new(key, value, seqno, black))), None);
        }

        let mut node = node.unwrap();
        node = Single::walkdown_rot23(node);
        match node.key.cmp(&key) {
            Ordering::Greater => {
                let (l, o) = Single::upsert(node.left, key, value, seqno, lsm);
                node.left = l;
                (Some(Single::walkuprot_23(node)), o)
            }
            Ordering::Less => {
                let (r, o) = Single::upsert(node.right, key, value, seqno, lsm);
                node.right = r;
                (Some(Single::walkuprot_23(node)), o)
            }
            Ordering::Equal => {
                let old_node = node.clone_detach();
                node.prepend_version(value, seqno, lsm);
                (Some(Single::walkuprot_23(node)), Some(old_node))
            }
        }
    }

    fn set_cas(
        llrb: &mut Llrb<K, V>,
        key: K,
        value: V,
        cas: u64,
    ) -> Result<Option<impl AsEntry<K, V>>, BognError> {
        let seqno = llrb.seqno + 1;
        let root = llrb.root.take();

        match Single::upsert_cas(root, key, value, cas, seqno, llrb.lsm) {
            (root, _, Some(err)) => {
                llrb.root = root;
                Err(err)
            }
            (Some(mut root), old_node, None) => {
                root.set_black();
                llrb.seqno = seqno;
                llrb.root = Some(root);
                if old_node.is_none() {
                    llrb.n_count += 1;
                }
                Ok(old_node)
            }
            _ => panic!("set_cas: impossible case, call programmer"),
        }
    }

    fn upsert_cas(
        node: Option<Box<Node<K, V>>>,
        key: K,
        val: V,
        cas: u64,
        seqno: u64,
        lsm: bool,
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

        let mut node = node.unwrap();
        node = Single::walkdown_rot23(node);
        let (old_node, err) = match node.key.cmp(&key) {
            Ordering::Greater => {
                let (k, v, left) = (key, val, node.left);
                let (l, o, e) = Single::upsert_cas(left, k, v, cas, seqno, lsm);
                node.left = l;
                (o, e)
            }
            Ordering::Less => {
                let (k, v, right) = (key, val, node.right);
                let (r, o, e) = Single::upsert_cas(right, k, v, cas, seqno, lsm);
                node.right = r;
                (o, e)
            }
            Ordering::Equal => {
                if node.is_deleted() && cas != 0 && cas != node.seqno() {
                    (None, Some(BognError::InvalidCAS))
                } else if !node.is_deleted() && cas != node.seqno() {
                    (None, Some(BognError::InvalidCAS))
                } else {
                    let old_node = node.clone_detach();
                    node.prepend_version(val, seqno, lsm);
                    (Some(old_node), None)
                }
            }
        };

        node = Single::walkuprot_23(node);
        return (Some(node), old_node, err);
    }

    fn delete<Q>(llrb: &mut Llrb<K, V>, key: &Q) -> Option<impl AsEntry<K, V>>
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        let seqno = llrb.seqno + 1;

        let lsm = llrb.lsm;
        if lsm {
            let root = llrb.root.take();
            let (root, old_node) = Single::delete_lsm(root, key, seqno);
            let mut root = root.unwrap();
            root.set_black();
            llrb.root = Some(root);

            if old_node.is_none() {
                llrb.n_count += 1;
                llrb.seqno = seqno;
            } else if !old_node.as_ref().unwrap().is_deleted() {
                llrb.seqno = seqno;
            }
            return old_node;
        }

        // in non-lsm mode remove the entry from the tree.
        let root = llrb.root.take();
        let (root, old_node) = match Single::do_delete(root, key) {
            (None, old_node) => (None, old_node),
            (Some(mut root), old_node) => {
                root.set_black();
                (Some(root), old_node)
            }
        };
        llrb.root = root;
        if old_node.is_some() {
            llrb.n_count -= 1;
            llrb.seqno = seqno
        }
        old_node
    }

    fn delete_lsm<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
        seqno: u64,
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>)
    where
        K: Borrow<Q> + From<Q>,
        Q: Clone + Ord + ?Sized,
    {
        if node.is_none() {
            // insert and mark as delete
            let (key, black) = (key.clone().into(), false);
            let mut node = Node::new(key, Default::default(), seqno, black);
            node.delete(seqno, true /*lsm*/);
            return (Some(Box::new(node)), None);
        }

        let mut node = node.unwrap();
        node = Single::walkdown_rot23(node);

        let (node, old_node) = match node.key.borrow().cmp(&key) {
            Ordering::Greater => {
                let (l, o) = Single::delete_lsm(node.left, key, seqno);
                node.left = l;
                (node, o)
            }
            Ordering::Less => {
                let (r, o) = Single::delete_lsm(node.right, key, seqno);
                node.right = r;
                (node, o)
            }
            Ordering::Equal => {
                let old_node = node.clone_detach();
                if node.is_deleted() {
                    (node, Some(old_node)) // noop
                } else {
                    node.delete(seqno, true /*lsm*/);
                    (node, Some(old_node))
                }
            }
        };

        (Some(Single::walkuprot_23(node)), old_node)
    }

    // this is the non-lsm path.
    fn do_delete<Q>(
        node: Option<Box<Node<K, V>>>,
        key: &Q,
    ) -> (Option<Box<Node<K, V>>>, Option<Node<K, V>>)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = match node {
            None => return (None, None),
            Some(node) => node,
        };

        if node.key.borrow().gt(key) {
            if node.left.is_none() {
                (Some(node), None)
            } else {
                let ok = !is_red(node.left_deref());
                if ok && !is_red(node.left.as_ref().unwrap().left_deref()) {
                    node = Single::move_red_left(node);
                }
                let (left, old_node) = Single::do_delete(node.left, key);
                node.left = left;
                (Some(Single::fixup(node)), old_node)
            }
        } else {
            if is_red(node.left_deref()) {
                node = Single::rotate_right(node);
            }

            if !node.key.borrow().lt(key) && node.right.is_none() {
                return (None, Some(*node));
            }

            let ok = node.right.is_some() && !is_red(node.right_deref());
            if ok && !is_red(node.right.as_ref().unwrap().left_deref()) {
                node = Single::move_red_right(node);
            }

            if !node.key.borrow().lt(key) {
                // node == key
                let (right, mut res_node) = Single::delete_min(node.right);
                node.right = right;
                if res_node.is_none() {
                    panic!("do_delete(): fatal logic, call the programmer");
                }
                let subdel = res_node.take().unwrap();
                let mut newnode = Box::new(subdel.clone_detach());
                newnode.left = node.left.take();
                newnode.right = node.right.take();
                newnode.black = node.black;
                (Some(Single::fixup(newnode)), Some(*node))
            } else {
                let (right, old_node) = Single::do_delete(node.right, key);
                node.right = right;
                (Some(Single::fixup(node)), old_node)
            }
        }
    }

    // return [node, old_node]
    fn delete_min(node: OBN<K, V>) -> (OBN<K, V>, Option<Node<K, V>>) {
        if node.is_none() {
            return (None, None);
        }
        let mut node = node.unwrap();
        if node.left.is_none() {
            return (None, Some(*node));
        }
        let left = node.left_deref();
        if !is_red(left) && !is_red(left.unwrap().left_deref()) {
            node = Single::move_red_left(node);
        }
        let (left, old_node) = Single::delete_min(node.left);
        node.left = left;
        (Some(Single::fixup(node)), old_node)
    }

    //--------- rotation routines for 2-3 algorithm ----------------

    fn walkdown_rot23(node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node
    }

    fn walkuprot_23(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_red(node.right_deref()) && !is_red(node.left_deref()) {
            node = Single::rotate_left(node);
        }
        let left = node.left_deref();
        if is_red(left) && is_red(left.unwrap().left_deref()) {
            node = Single::rotate_right(node);
        }
        if is_red(node.left_deref()) && is_red(node.right_deref()) {
            Single::flip(node.deref_mut())
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
    fn rotate_left(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_black(node.right_deref()) {
            panic!("rotateleft(): rotating a black link ? call the programmer");
        }
        let mut x = node.right.unwrap();
        node.right = x.left;
        x.black = node.black;
        node.set_red();
        x.left = Some(node);
        x
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
    fn rotate_right(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        if is_black(node.left_deref()) {
            panic!("rotateright(): rotating a black link ? call the programmer")
        }
        let mut x = node.left.unwrap();
        node.left = x.right;
        x.black = node.black;
        node.set_red();
        x.right = Some(node);
        x
    }

    //        (x)                   (!x)
    //         |                     |
    //        node                  node
    //        / \                   / \
    //      (y) (z)              (!y) (!z)
    //     /      \              /      \
    //   left    right         left    right
    //
    fn flip(node: &mut Node<K, V>) {
        node.left.as_mut().unwrap().toggle_link();
        node.right.as_mut().unwrap().toggle_link();
        node.toggle_link();
    }

    fn fixup(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        node = if is_red(node.right_deref()) {
            Single::rotate_left(node)
        } else {
            node
        };
        node = {
            let left = node.left_deref();
            if is_red(left) && is_red(left.unwrap().left_deref()) {
                Single::rotate_right(node)
            } else {
                node
            }
        };
        if is_red(node.left_deref()) && is_red(node.right_deref()) {
            Single::flip(node.deref_mut());
        }
        node
    }

    fn move_red_left(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        Single::flip(node.deref_mut());
        if is_red(node.right.as_ref().unwrap().left_deref()) {
            node.right = Some(Single::rotate_right(node.right.take().unwrap()));
            node = Single::rotate_left(node);
            Single::flip(node.deref_mut());
        }
        node
    }

    fn move_red_right(mut node: Box<Node<K, V>>) -> Box<Node<K, V>> {
        Single::flip(node.deref_mut());
        if is_red(node.left.as_ref().unwrap().left_deref()) {
            node = Single::rotate_right(node);
            Single::flip(node.deref_mut());
        }
        node
    }
}
