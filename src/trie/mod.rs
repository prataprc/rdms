mod trie;

pub use trie::Trie;

#[derive(Copy, Clone)]
pub enum WalkRes {
    Ok,
    SkipDepth,
    SkipBreath,
    SkipBoth,
}
