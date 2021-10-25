pub const GIT_DIR: &'static str = ".git";

mod config;
mod index;

pub use config::{Config, Permissions};
pub use index::Index;
