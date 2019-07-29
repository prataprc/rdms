use crate::core::{Diff, Serialize};
use crate::robt_config::Config;
use crate::robt_snap::Snapshot;

pub struct Robt<K, V>
where
    K: Clone + Ord + Serialize,
    V: Clone + Diff + Serialize,
    <V as Diff>::D: Serialize,
{
    config: Config,
    levels: Vec<Snapshot<K, V>>,
}
