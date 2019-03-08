use crate::traits::Diff;

impl Diff for i64 {
    type D = i64;

    /// O - N = D
    fn diff(&self, _a: &Self) -> Self::D {
        self.clone()
    }

    /// N + D = O
    fn merge(&self, a: &Self::D) -> Self {
        a.clone()
    }
}
