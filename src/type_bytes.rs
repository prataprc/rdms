use std::convert::TryInto;

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

// 4 byte header, encoding the length of payload followed by
// the actual payload.
impl Serialize for Vec<u8> {
    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let hdr1: u32 = self.len().try_into().unwrap();
        let scratch = hdr1.to_be_bytes();

        let mut n = buf.len();
        buf.resize(n + scratch.len() + self.len(), 0);
        buf[n..n + scratch.len()].copy_from_slice(&scratch);
        n += scratch.len();
        buf[n..].copy_from_slice(self);
        scratch.len() + self.len()
    }

    fn decode(&mut self, buf: &[u8]) -> Result<usize, Error> {
        if buf.len() < 4 {
            let msg = format!("bytes decode header {} < 4", buf.len());
            return Err(Error::DecodeFail(msg));
        }
        let len: usize = u32::from_be_bytes(buf[..4].try_into().unwrap())
            .try_into()
            .unwrap();
        if buf.len() < (len + 4) {
            let msg = format!("bytes decode payload {} < {}", buf.len(), len);
            return Err(Error::DecodeFail(msg));
        }
        self.resize(len, 0);
        self.copy_from_slice(&buf[4..len + 4]);
        Ok(len + 4)
    }
}
