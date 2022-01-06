//! Module implement [dba::AsKey] trait for [String], [path::Path] [path::PathBuf].
//!
//! And custom types that are handy to use as DBA store keys. `Str`.

use std::path;

use crate::{dba, Error, Result};

/// Type is a convenience type to wrap any ordinary string into path components.
///
/// EG: a string like `hello-world` shall be converted to ["h", "e", "l", "hello-word"]
/// when a depth of `3` is used. For a depth `4`, it shall be converted to
/// ["h", "e", "l", "l", "hello-word"]. Use [dba::AsKey] trait for conversion.
pub struct Str {
    key: String,
    depth: usize,
}

impl Str {
    /// Default depth to convert any string into path components.
    pub const DEFAULT_DEPTH: usize = 3;
}

impl<'a> From<(&'a str, usize)> for Str {
    fn from((key, depth): (&str, usize)) -> Str {
        Str {
            key: key.to_string(),
            depth,
        }
    }
}

impl<'a> From<&'a str> for Str {
    fn from(key: &str) -> Str {
        Str {
            key: key.to_string(),
            depth: Str::DEFAULT_DEPTH,
        }
    }
}

impl<'a> From<String> for Str {
    fn from(key: String) -> Str {
        Str {
            key: key,
            depth: Str::DEFAULT_DEPTH,
        }
    }
}

impl dba::AsKey for Str {
    fn to_key_path(&self) -> Result<Vec<String>> {
        let parts = match self.key.len() {
            0 => vec![],
            _ => {
                let mut parts: Vec<String> = self
                    .key
                    .chars()
                    .take(self.depth)
                    .map(|ch| ch.to_string())
                    .collect();

                parts.push(self.key.to_string());
                parts
            }
        };

        Ok(parts)
    }
}

impl dba::AsKey for String {
    fn to_key_path(&self) -> Result<Vec<String>> {
        Ok(self.split('/').map(ToString::to_string).collect())
    }
}

impl dba::AsKey for path::Path {
    fn to_key_path(&self) -> Result<Vec<String>> {
        let mut items = vec![];
        for c in self.components() {
            match c {
                path::Component::Normal(c) => match c.to_str() {
                    Some(c) => items.push(c.to_string()),
                    None => err_at!(InvalidInput, msg: "key {:?} is invalid", self)?,
                },
                _ => err_at!(InvalidInput, msg: "key {:?} is invalid", self)?,
            }
        }
        Ok(items)
    }
}

impl dba::AsKey for path::PathBuf {
    fn to_key_path(&self) -> Result<Vec<String>> {
        let mut items = vec![];
        for c in self.components() {
            match c {
                path::Component::Normal(c) => match c.to_str() {
                    Some(c) => items.push(c.to_string()),
                    None => err_at!(InvalidInput, msg: "key {:?} is invalid", self)?,
                },
                _ => err_at!(InvalidInput, msg: "key {:?} is invalid", self)?,
            }
        }
        Ok(items)
    }
}
