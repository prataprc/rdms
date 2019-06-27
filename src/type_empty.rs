use crate::core::{Diff, Serialize};
use crate::error::Error;

/// Empty value, can be used for indexing entries that have a
/// key but no value.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct Empty;

impl Diff for Empty {
    type D = Empty;

    /// D = N - O
    fn diff(&self, _a: &Self) -> Self::D {
        Empty
    }

    /// O = N - D
    fn merge(&self, _a: &Self::D) -> Self {
        Empty
    }
}

impl Serialize for Empty {
    fn encode(&self, _buf: &mut Vec<u8>) -> usize {
        0
    }

    fn decode(&mut self, _buf: &[u8]) -> Result<usize, Error> {
        Ok(0)
    }
}
