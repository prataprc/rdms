use std::path;

use crate::{dba::AsKey, Error, Result};

impl AsKey for String {
    fn to_key_path(&self) -> Result<Vec<String>> {
        Ok(self.split('/').map(ToString::to_string).collect())
    }
}

impl AsKey for path::Path {
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
