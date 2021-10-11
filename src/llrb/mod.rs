//! Module implement Left leaning red black tree with multi-reader support.

mod depth;
mod index;
mod node;
mod op;
mod stats;

pub use depth::Depth;
pub use index::Index;
use node::Node;
pub use op::Write;
pub use stats::Stats;

#[cfg(any(test, feature = "rdms-perf"))]
pub use index::load_index;
