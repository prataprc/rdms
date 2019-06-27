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
        let n = buf.len();
        buf.resize(n + 4, 0);
        buf[n..].copy_from_slice(&self.to_be_bytes());
        4
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize, Error> {
        if buf.len() >= 4 {
            let mut scratch = [0_u8; 4];
            scratch.copy_from_slice(&buf[..4]);
            *self = i32::from_be_bytes(scratch);
            Ok(4)
        } else {
            Err(Error::DecodeFail(format!("i32 encoded len {}", buf.len())))
        }
    }
}
