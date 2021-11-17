use serde::{Deserialize, Serialize};

use rdms::dba;

#[derive(Debug, Deserialize, Serialize)]
pub struct Crate {
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    created_at: String,
    description: String,
    documentation: String, // url
    downloads: usize,
    homepage: String, // url
    id: String,
    max_upload_size: Option<usize>,
    name: String,
    readme: String,
    repository: String, // url
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    updated_at: String,
}

pub const CRATE_RECORD_EXT: &str = ".json";
pub const CRATE_TABLE: &str = "#crates";
pub const CRATE_KEY_LEVELS: usize = 3;
impl Crate {
    pub fn to_crate_key(&self) -> Option<String> {
        assert!(!self.name.contains('/'));

        let mut full_key = CRATE_TABLE.to_string();

        match dba::to_content_key(&self.name, CRATE_KEY_LEVELS) {
            Some(key) => {
                full_key.push('/');
                full_key.push_str(&key);
                full_key.push_str(CRATE_RECORD_EXT);
                Some(full_key)
            }
            None => None,
        }
    }
}
