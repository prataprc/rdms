use crate::core::{Diff, Serialize};
use crate::error::Error;

impl Diff for Vec<u8> {
    type D = Vec<u8>;

    /// D = N - O
    fn diff(&self, old: &Self) -> Self::D {
        old.clone()
    }

    /// O = N - D
    fn merge(&self, delta: &Self::D) -> Self {
        delta.clone()
    }
}

impl Serialize for Vec<u8> {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.resize(self.len(), 0);
        buf.copy_from_slice(self);
    }

    fn decode(&mut self, buf: &[u8]) -> Result<(), Error> {
        self.resize(buf.len(), 0);
        self.copy_from_slice(buf);
        Ok(())
    }
}
