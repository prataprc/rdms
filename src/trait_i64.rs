use crate::error::BognError;
use crate::traits::{Diff, Serialize};

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

impl Serialize for i64 {
    fn encode(&self, mut buf: Vec<u8>) -> Vec<u8> {
        buf.resize(8, 0);
        buf.copy_from_slice(&self.to_be_bytes()[..8]);
        buf
    }

    fn decode(&mut self, buf: &[u8]) -> Result<(), BognError> {
        let mut scratch = [0_u8; 8];
        scratch.copy_from_slice(&buf[..8]);
        *self = i64::from_be_bytes(scratch);
        Ok(())
    }
}
