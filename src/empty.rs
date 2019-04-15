use crate::traits::{Diff, Serialize};

/// Empty value, can be used for indexing entries that have a
/// key but no value.
#[derive(Copy, Clone, Default, Eq, PartialEq)]
pub struct Empty;

impl Diff for Empty {
    type D = Empty;

    /// O - N = D
    fn diff(&self, _a: &Self) -> Self::D {
        Empty
    }

    /// N + D = O
    fn merge(&self, _a: &Self::D) -> Self {
        Empty
    }
}

impl Serialize for Empty {
    fn encode(&self, buf: Vec<u8>) -> Vec<u8> {
        buf
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Empty)
    }
}
