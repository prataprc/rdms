use crate::{trie::WalkRes, Result};

// TODO: remove fmt::Debug

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
        P: Ord + std::fmt::Debug,
        V: Clone,
    {
        match comps {
            [] => None,
            comps => match self.root.remove(comps) {
                (Some(value), _) => {
                    self.n_count -= 1;
                    Some(value)
                }
                (None, _) => None,
            },
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

    pub fn walk<S, F>(&self, mut state: S, callb: F) -> Result<S>
    where
        P: Clone,
        V: Clone,
        F: Fn(&mut S, &[P], &P, Option<&V>, usize, usize) -> Result<WalkRes>,
    {
        state = match &self.root {
            Node::Root { children } => {
                let mut iter = children.iter().enumerate();
                loop {
                    state = match iter.next() {
                        Some((breath, child)) => {
                            match child.do_walk(vec![], state, &callb, 0, breath)? {
                                (state, true) => break state,
                                (state, false) => state,
                            }
                        }
                        None => break state,
                    }
                }
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
                let oldv = value.take();
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
        P: Ord + std::fmt::Debug,
        V: Clone,
    {
        match comps {
            [] => match self {
                Node::Comp {
                    children, value, ..
                } => {
                    let oldv = value.take();
                    *value = None;
                    (oldv, children.is_empty())
                }
                _ => unreachable!(),
            },
            [comp] | [comp, ..] => {
                let res = self.as_children().binary_search_by_key(&comp, |n| match n {
                    Node::Comp { comp, .. } => comp,
                    _ => unreachable!(),
                });
                match res {
                    Ok(off) => {
                        let (value, rm) = self.as_mut_children()[off].remove(&comps[1..]);
                        if rm {
                            self.as_mut_children().remove(off);
                        }
                        match self {
                            Node::Comp {
                                children,
                                value: None,
                                ..
                            } if children.is_empty() => (value, true),
                            _ => (value, false),
                        }
                    }
                    Err(_off) => (None, false),
                }
            }
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
            [] => None,
        }
    }

    // Result (state, skip_breath)
    fn do_walk<S, F>(
        &self,
        mut parent: Vec<P>,
        mut state: S,
        callb: &F,
        depth: usize,
        breath: usize,
    ) -> Result<(S, bool)>
    where
        P: Clone,
        V: Clone,
        F: Fn(&mut S, &[P], &P, Option<&V>, usize, usize) -> Result<WalkRes>,
    {
        let (comp, value, children) = match self {
            Node::Comp {
                comp,
                value,
                children,
            } => (comp, value.as_ref(), children),
            _ => unreachable!(),
        };

        let res = callb(&mut state, &parent, comp, value, depth, breath)?;

        let state = match res {
            WalkRes::Ok | WalkRes::SkipBreath => {
                parent.push(comp.clone());

                let mut iter = children.iter().enumerate();
                loop {
                    state = match iter.next() {
                        Some((breath, child)) => {
                            let parent = parent.clone();
                            match child.do_walk(
                                parent,
                                state,
                                callb,
                                depth + 1,
                                breath,
                            )? {
                                (state, true) => break state,
                                (state, false) => state,
                            }
                        }
                        None => break state,
                    };
                }
            }
            WalkRes::SkipDepth | WalkRes::SkipBoth => state,
        };

        Ok((
            state,
            matches!(res, WalkRes::SkipBreath | WalkRes::SkipBoth),
        ))
    }
}

#[cfg(test)]
#[path = "trie_test.rs"]
mod trie_test;
