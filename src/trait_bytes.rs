use crate::traits::Diff;

impl Diff for Vec<u8> {
    type D = Vec<u8>;

    /// O - N = D
    fn diff(&self, _a: &Self) -> Self::D {
        self.clone()
    }

    /// N + D = O
    fn merge(&self, a: &Self::D) -> Self {
        a.clone()
    }
}
