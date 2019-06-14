use crate::core::{Diff, Serialize};
use crate::error::Error;

impl Diff for i32 {
    type D = i32;

    /// D = N - O
    fn diff(&self, old: &Self) -> Self::D {
        old.clone()
    }

    /// O = N - D
    fn merge(&self, delta: &Self::D) -> Self {
        delta.clone()
    }
}

impl Serialize for i32 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        buf.resize(4, 0);
        buf.copy_from_slice(&self.to_be_bytes()[..4]);
        4
    }

    fn decode(&mut self, buf: &[u8]) -> Result<(), Error> {
        let mut scratch = [0_u8; 4];
        scratch.copy_from_slice(&buf[..4]);
        *self = i32::from_be_bytes(scratch);
        Ok(())
    }
}
