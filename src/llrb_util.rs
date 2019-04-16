use crate::llrb_depth::Depth;

/// Statistics on LLRB tree.
#[derive(Default)]
pub struct Stats {
    entries: usize, // number of entries in the tree.
    node_size: usize,
    blacks: Option<usize>,
    depths: Option<Depth>,
}

impl Stats {
    pub(crate) fn new(entries: usize, node_size: usize) -> Stats {
        Stats {
            entries,
            node_size,
            blacks: None,
            depths: None,
        }
    }

    #[inline]
    pub(crate) fn set_blacks(&mut self, blacks: usize) {
        self.blacks = Some(blacks)
    }

    #[inline]
    pub(crate) fn set_depths(&mut self, depths: Depth) {
        self.depths = Some(depths)
    }

    #[inline]
    pub fn entries(&self) -> usize {
        self.entries
    }

    #[inline]
    pub fn node_size(&self) -> usize {
        self.node_size
    }

    #[inline]
    pub fn blacks(&self) -> Option<usize> {
        self.blacks
    }

    #[inline]
    pub(crate) fn sample_depth(&mut self, depth: usize) {
        self.depths.as_mut().unwrap().sample(depth)
    }

    pub fn depths(&self) -> Option<Depth> {
        if self.depths.as_ref().unwrap().samples() == 0 {
            None
        } else {
            self.depths.clone()
        }
    }
}
