pub mod filter;
pub mod filter_map;
pub mod map;
pub mod reduce;
pub mod sink;
pub mod source;

pub trait Message {
    fn finish() -> Self;
}
