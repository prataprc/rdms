use crate::Result;

pub struct Trie<P, V> {
    root: Node<P, V>,
    n_count: usize,
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
        Trie { root, n_count: 0 }
    }

    pub fn as_root(&self) -> &Node<P, V> {
        &self.root
    }

    pub fn set(&mut self, comps: &[P], value: V) -> Option<V>
    where
        P: Clone + Ord,
        V: Clone,
    {
        match self.root.set(comps, value) {
            res @ Some(_) => res,
            None => {
                self.n_count += 1;
                None
            }
        }
    }

    pub fn remove(&mut self, comps: &[P]) -> Option<V>
    where
        P: Ord,
        V: Clone,
    {
        match self.root.remove(comps) {
            (Some(value), _) => {
                self.n_count -= 1;
                Some(value)
            }
            (None, _) => None,
        }
    }

    pub fn len(&self) -> usize {
        self.n_count
    }

    pub fn is_empty(&self) -> bool {
        self.n_count == 0
    }

    pub fn get(&self, comps: &[P]) -> Option<&V>
    where
        P: Ord,
    {
        self.root.get(comps)
    }

    pub fn walk<S, F>(&self, mut state: S, callb: &mut F) -> Result<S>
    where
        P: Clone,
        V: Clone,
        F: FnMut(&mut S, &[P], &P, Option<&V>, usize, usize) -> Result<WalkRes>,
    {
        let state = match &self.root {
            Node::Root { children } => {
                for (breath, child) in children.iter().enumerate() {
                    state = child.do_walk(vec![], state, callb, 0, breath)?;
                }
                state
            }
            _ => unreachable!(),
        };

        Ok(state)
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
    fn new(comp: &P, value: Option<V>) -> Node<P, V>
    where
        P: Clone,
        V: Clone,
    {
        Node::Comp {
            comp: comp.clone(),
            value,
            children: Vec::default(),
        }
    }

    fn as_value(&self) -> Option<&V> {
        match self {
            Node::Root { .. } => None,
            Node::Comp { value: None, .. } => None,
            Node::Comp {
                value: Some(value), ..
            } => Some(value),
        }
    }

    fn as_children(&self) -> &[Node<P, V>] {
        match self {
            Node::Root { children, .. } => children,
            Node::Comp { children, .. } => children,
        }
    }

    fn as_mut_children(&mut self) -> &mut Vec<Node<P, V>> {
        match self {
            Node::Root { children, .. } => children,
            Node::Comp { children, .. } => children,
        }
    }

    fn set_value(&mut self, val: Option<V>) -> Option<V>
    where
        V: Clone,
    {
        match self {
            Node::Root { .. } => unreachable!(),
            Node::Comp { value, .. } => {
                let oldv = value.clone();
                *value = val;
                oldv
            }
        }
    }

    fn set(&mut self, comps: &[P], value: V) -> Option<V>
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
                        self.as_mut_children()[off].set_value(Some(value))
                    }
                    Ok(off) => self.as_mut_children()[off].set(&comps[1..], value),
                    Err(off) if comps.len() == 1 => {
                        self.as_mut_children()
                            .insert(off, Node::new(comp, Some(value)));
                        None
                    }
                    Err(off) => {
                        self.as_mut_children().insert(off, Node::new(comp, None));
                        self.as_mut_children()[off].set(&comps[1..], value)
                    }
                }
            }
            [] => None,
        }
    }

    fn remove(&mut self, comps: &[P]) -> (Option<V>, bool)
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
                let value = match res {
                    Ok(off) if comps.len() == 1 => {
                        match self.as_mut_children().remove(off) {
                            Node::Comp {
                                comp,
                                value,
                                children,
                            } if children.is_empty() => {
                                self.as_mut_children().insert(
                                    off,
                                    Node::Comp {
                                        comp,
                                        value: None,
                                        children,
                                    },
                                );
                                value
                            }
                            Node::Comp { value, .. } => value,
                            _ => unreachable!(),
                        }
                    }
                    Ok(off) => match self.as_mut_children()[off].remove(&comps[1..]) {
                        (value, true) => {
                            self.as_mut_children().remove(off);
                            value
                        }
                        (value, _) => value,
                    },
                    Err(_off) => None,
                };

                match value {
                    None => (None, false),
                    val if self.as_value().is_none() && self.as_children().is_empty() => {
                        (val, true)
                    }
                    val => (val, false),
                }
            }
            [] => unreachable!(),
        }
    }

    fn get(&self, comps: &[P]) -> Option<&V>
    where
        P: Ord,
    {
        match comps {
            [comp] | [comp, ..] => {
                let res = self.as_children().binary_search_by_key(&comp, |n| match n {
                    Node::Comp { comp, .. } => comp,
                    _ => unreachable!(),
                });
                match res {
                    Ok(off) if comps.len() == 1 => self.as_children()[off].as_value(),
                    Ok(off) => self.as_children()[off].get(&comps[1..]),
                    Err(_off) => None,
                }
            }
            [] => unreachable!(),
        }
    }

    fn do_walk<S, F>(
        &self,
        mut parent: Vec<P>,
        mut state: S,
        callb: &mut F,
        depth: usize,
        breath: usize,
    ) -> Result<S>
    where
        P: Clone,
        V: Clone,
        F: FnMut(&mut S, &[P], &P, Option<&V>, usize, usize) -> Result<WalkRes>,
    {
        let (comp, value, children) = match self {
            Node::Comp {
                comp,
                value,
                children,
            } => (comp, value.as_ref(), children),
            _ => unreachable!(),
        };

        let state = match callb(&mut state, &parent, comp, value, depth, breath)? {
            WalkRes::Ok => {
                parent.push(comp.clone());

                for (breath, child) in children.iter().enumerate() {
                    let parent = parent.clone();
                    state = child.do_walk(parent, state, callb, depth + 1, breath)?;
                }
                state
            }
            WalkRes::Skip => state,
        };

        Ok(state)
    }
}

pub enum WalkRes {
    Ok,
    Skip,
}

#[cfg(test)]
#[path = "trie_test.rs"]
mod trie_test;
