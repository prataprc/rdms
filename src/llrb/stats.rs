use std::{fmt, result};

#[allow(unused_imports)]
use crate::llrb::Index;
use crate::{db, llrb::Depth, util::spinlock};

/// Statistic type, for [Index] type.
pub struct Stats {
    pub name: String,
    pub spin: bool,
    pub node_size: usize,
    pub n_count: usize,
    pub n_deleted: usize,
    pub tree_footprint: isize,
    pub spin_stats: spinlock::Stats,
    pub blacks: Option<usize>,
    pub depths: Option<Depth>,
}

impl Stats {
    pub(crate) fn new(name: &str, spin: bool) -> Stats {
        Stats {
            name: name.to_string(),
            spin,
            node_size: Default::default(),
            n_count: Default::default(),
            n_deleted: Default::default(),
            tree_footprint: Default::default(),
            spin_stats: Default::default(),
            blacks: None,
            depths: None,
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let none = "none".to_string();
        let b = self.blacks.as_ref().map_or(none.clone(), |x| x.to_string());
        let d = self.depths.as_ref().map_or(none, |x| x.to_string());
        writeln!(f, "llrb.name = {}", self.name)?;
        writeln!(
            f,
            "llrb = {{ n_count={}, n_deleted={} node_size={}, blacks={} }}",
            self.n_count, self.n_deleted, self.node_size, b,
        )?;
        writeln!(f, "llrb = {{ tree_footprint={} }}", self.tree_footprint)?;
        writeln!(f, "llrb.spin_stats = {}", self.spin_stats)?;
        writeln!(f, "llrb.depths = {}", d)
    }
}

impl db::ToJson for Stats {
    fn to_json(&self) -> String {
        let null = "null".to_string();
        // TODO: should we convert this to to_json() ?
        let spin_stats = self.spin_stats.to_string();
        format!(
            concat!(
                r#"{{ ""llrb": {{ "name": {}, "n_count": {:X}, "#,
                r#""n_deleted": {}, "#,
                r#""tree_footprint": {}, "#,
                r#""node_size": {}, "spin_stats": {}, "#,
                r#""blacks": {}, "depths": {} }} }}"#,
            ),
            self.name,
            self.n_count,
            self.n_deleted,
            self.tree_footprint,
            self.node_size,
            spin_stats,
            self.blacks
                .as_ref()
                .map_or(null.clone(), |x| format!("{}", x)),
            self.depths.as_ref().map_or(null, |x| x.to_json()),
        )
    }
}
