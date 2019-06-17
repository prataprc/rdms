use crate::core::{Diff, Serialize};
use crate::error::Error;

impl Diff for i64 {
    type D = i64;

    /// D = N - O
    fn diff(&self, old: &Self) -> Self::D {
        old.clone()
    }

    /// O = N - D
    fn merge(&self, delta: &Self::D) -> Self {
        delta.clone()
    }
}

impl Serialize for i64 {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let n = buf.len();
        buf.resize(n + 8, 0);
        buf[n..].copy_from_slice(&self.to_be_bytes());
        8
    }

    fn decode(&mut self, buf: &[u8]) -> Result<(), Error> {
        if buf.len() >= 8 {
            let mut scratch = [0_u8; 8];
            scratch.copy_from_slice(&buf[..8]);
            *self = i64::from_be_bytes(scratch);
            Ok(())
        } else {
            Err(Error::DecodeFail(format!("i64 encoded len {}", buf.len())))
        }
    }
}
