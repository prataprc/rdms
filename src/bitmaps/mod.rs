//! Module implement adaptors for popular bitmap filters.

mod croaring;
mod fuse16;
mod fuse8;
mod nobitmap;
mod xor8;

pub use self::croaring::CRoaring;
pub use nobitmap::NoBitmap;
// Re-imported from xorfilter package.
pub use xorfilter::Fuse16;
// Re-imported from xorfilter package.
pub use xorfilter::Fuse8;
// Re-imported from xorfilter package.
pub use xorfilter::Xor8;
