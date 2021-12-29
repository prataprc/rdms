pub mod filter;
pub mod filter_map;
pub mod map;
pub mod reduce;
pub mod sink;
pub mod source;
pub mod split;

const DEFAULT_CHAN_SIZE: usize = 1024;

pub trait Message {
    fn finish() -> Self;
}
