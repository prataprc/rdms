pub struct Trie {
    root: Node,
}

impl Trie {
    pub fn new() -> Trie {
        Trie {
            root: "--root--".to_string().into(),
        }
    }

    pub fn insert(&mut self, comps: &[String], value: &[u8]) {
        self.root.insert(comps, value);
    }

    pub fn remove(&mut self, comps: &[String]) {
        self.root.remove(comps)
    }

    pub fn as_root(&self) -> &Node {
        &self.root
    }
}

pub struct Node {
    comp: String,
    children: Vec<Node>,
    leafs: Vec<Op>,
}

pub enum Op {
    /// insert leaf component
    Ins { comp: String, value: Vec<u8> },
    /// Remove leaf component
    Rem { comp: String },
}

impl From<String> for Node {
    fn from(comp: String) -> Node {
        Node {
            comp,
            children: Vec::default(),
            leafs: Vec::default(),
        }
    }
}

impl Node {
    pub fn as_comp(&self) -> &str {
        &self.comp
    }

    pub fn as_children(&self) -> &[Node] {
        &self.children
    }

    pub fn as_leafs(&self) -> &[Op] {
        &self.leafs
    }

    pub fn insert(&mut self, comps: &[String], value: &[u8]) {
        match comps {
            [comp] => {
                let res = self.leafs.binary_search_by_key(&comp, |w| match w {
                    Op::Ins { comp, .. } => comp,
                    Op::Rem { comp } => comp,
                });
                let off = match res {
                    Ok(off) => off,
                    Err(off) => off,
                };
                let w = Op::Ins {
                    comp: comp.to_string(),
                    value: value.to_vec(),
                };
                self.leafs.insert(off, w);
            }
            [comp, ..] => {
                let res = self.children.binary_search_by_key(&comp, |n| &n.comp);
                let off = match res {
                    Ok(off) => off,
                    Err(off) => {
                        self.children.insert(off, comp.clone().into());
                        off
                    }
                };
                self.children[off].insert(&comps[1..], value);
            }
            [] => unreachable!(),
        }
    }

    fn remove(&mut self, comps: &[String]) {
        match comps {
            [comp] => {
                let res = self.leafs.binary_search_by_key(&comp, |w| match w {
                    Op::Ins { comp, .. } => comp,
                    Op::Rem { comp } => comp,
                });
                let off = match res {
                    Ok(off) => off,
                    Err(off) => off,
                };
                let w = Op::Rem {
                    comp: comp.to_string(),
                };
                self.leafs.insert(off, w);
            }
            [comp, ..] => {
                let res = self.children.binary_search_by_key(&comp, |n| &n.comp);
                let off = match res {
                    Ok(off) => off,
                    Err(off) => {
                        self.children.insert(off, comp.clone().into());
                        off
                    }
                };
                self.children[off].remove(&comps[1..]);
            }
            [] => unreachable!(),
        }
    }
}
