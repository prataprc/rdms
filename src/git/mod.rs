use std::path;

pub const GIT_DIR: &'static str = ".git";

trait ToKey {
    type Key;

    fn to_key(&self) -> Self::Key;
}

trait Keyable {
    fn to_path(&self) -> path::Path;
}

mod config;
mod index;

pub use config::{Config, Permissions};
pub use index::Index;
