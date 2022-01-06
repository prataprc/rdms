pub struct Trie<P, V> {
    root: Node<P, V>,
}

impl<P, V> Default for Trie<P, V> {
    fn default() -> Self {
        Trie::new()
    }
}

impl<P, V> Trie<P, V> {
    pub fn new() -> Trie<P, V> {
        let root = Node::Root {
            children: Vec::default(),
        };
        Trie { root }
    }

    pub fn insert(&mut self, comps: &[P], value: &V)
    where
        P: Clone + Ord,
        V: Clone,
    {
        self.root.insert(comps, value);
    }

    pub fn remove(&mut self, comps: &[P])
    where
        P: Ord,
        V: Clone,
    {
        self.root.remove(comps)
    }

    pub fn as_root(&self) -> &Node<P, V> {
        &self.root
    }
}

pub enum Node<P, V> {
    Root {
        children: Vec<Node<P, V>>,
    },
    Comp {
        comp: P,
        value: Option<V>,
        children: Vec<Node<P, V>>,
    },
}

impl<P, V> Node<P, V> {
    pub fn new(comp: &P, value: Option<&V>) -> Node<P, V>
    where
        P: Clone,
        V: Clone,
    {
        Node::Comp {
            comp: comp.clone(),
            value: value.cloned(),
            children: Vec::default(),
        }
    }

    pub fn as_comp(&self) -> Option<&P> {
        match self {
            Node::Root { .. } => None,
            Node::Comp { comp, .. } => Some(comp),
        }
    }

    pub fn as_value(&self) -> Option<&V> {
        match self {
            Node::Root { .. } => None,
            Node::Comp { value: None, .. } => None,
            Node::Comp {
                value: Some(value), ..
            } => Some(value),
        }
    }

    pub fn as_children(&self) -> &[Node<P, V>] {
        match self {
            Node::Root { children, .. } => children,
            Node::Comp { children, .. } => children,
        }
    }

    pub fn as_mut_children(&mut self) -> &mut Vec<Node<P, V>> {
        match self {
            Node::Root { children, .. } => children,
            Node::Comp { children, .. } => children,
        }
    }

    pub fn set_value(&mut self, val: Option<&V>)
    where
        V: Clone,
    {
        match self {
            Node::Root { .. } => unreachable!(),
            Node::Comp { value, .. } => *value = val.cloned(),
        }
    }

    pub fn insert(&mut self, comps: &[P], value: &V)
    where
        P: Clone + Ord,
        V: Clone,
    {
        match comps {
            [comp] | [comp, ..] => {
                let res = self.as_children().binary_search_by_key(&comp, |n| match n {
                    Node::Comp { comp, .. } => comp,
                    _ => unreachable!(),
                });
                match res {
                    Ok(off) if comps.len() == 1 => {
                        self.as_mut_children()[off].set_value(Some(value));
                    }
                    Ok(off) => self.as_mut_children()[off].insert(&comps[1..], value),
                    Err(off) if comps.len() == 1 => self
                        .as_mut_children()
                        .insert(off, Node::new(comp, Some(value))),
                    Err(off) => {
                        self.as_mut_children().insert(off, Node::new(comp, None));
                        self.as_mut_children()[off].insert(&comps[1..], value);
                    }
                }
            }
            [] => unreachable!(),
        }
    }

    fn remove(&mut self, comps: &[P])
    where
        P: Ord,
        V: Clone,
    {
        match comps {
            [comp] | [comp, ..] => {
                let res = self.as_children().binary_search_by_key(&comp, |n| match n {
                    Node::Comp { comp, .. } => comp,
                    _ => unreachable!(),
                });
                match res {
                    Ok(off) if comps.len() == 1 => {
                        self.as_mut_children().remove(off);
                    }
                    Ok(off) => {
                        self.as_mut_children()[off].set_value(None);
                        self.as_mut_children()[off].remove(&comps[1..]);
                    }
                    Err(_off) => (),
                };
            }
            [] => unreachable!(),
        }
    }
}
