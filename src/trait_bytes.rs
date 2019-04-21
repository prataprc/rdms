use crate::error::BognError;
use crate::traits::{Diff, Serialize};

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

impl Serialize for Vec<u8> {
    fn encode(&self, buf: Vec<u8>) -> Vec<u8> {
        buf
    }

    fn decode(&mut self, buf: &[u8]) -> Result<(), BognError> {
        self.resize(buf.len(), 0);
        self.copy_from_slice(buf);
        Ok(())
    }
}
