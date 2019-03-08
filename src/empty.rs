use crate::traits::Diff;

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
