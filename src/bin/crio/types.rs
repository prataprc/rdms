use serde::{Deserialize, Serialize};

use rdms::{
    dba::{self, AsKey},
    err_at, Error, Result,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Crate {
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    created_at: String,
    description: String,
    documentation: String, // url
    downloads: usize,
    homepage: String, // url
    id: u64,
    max_upload_size: Option<usize>,
    name: String,
    readme: String,
    repository: String, // url
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    updated_at: String,
}

pub const CRATE_RECORD_EXT: &str = ".json";
pub const CRATE_TABLE: &str = "table:crates";
impl Crate {
    pub fn to_key(&self) -> Option<String> {
        assert!(!self.name.contains('/'));

        let mut parts = vec![CRATE_TABLE.to_string()];
        let key: dba::Str = self.name.clone().into();
        parts.extend_from_slice(&key.to_key_path().ok()?);
        Some(parts.join("/") + CRATE_RECORD_EXT)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Category {
    category: String,
    crates_cnt: usize,
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    created_at: String,
    description: String,
    id: u64,
    path: String,
    slug: String,
}

pub const CATEGORY_RECORD_EXT: &str = ".json";
pub const CATEGORY_TABLE: &str = "table:categories";
impl Category {
    pub fn to_key(&self) -> Option<String> {
        assert!(!self.path.contains('/'));

        let mut full_key = CATEGORY_TABLE.to_string();
        full_key.push('/');
        full_key.push_str(&self.path);
        full_key.push_str(CATEGORY_RECORD_EXT);
        Some(full_key)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Keyword {
    crates_cnt: usize,
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    created_at: String,
    id: u64,
    keyword: String,
}

pub const KEYWORDS_RECORD_EXT: &str = ".json";
pub const KEYWORDS_TABLE: &str = "table:keywords";
impl Keyword {
    pub fn to_key(&self) -> Option<String> {
        assert!(!self.keyword.contains('/'));

        let mut parts = vec![KEYWORDS_TABLE.to_string()];
        let key: dba::Str = self.keyword.clone().into();
        parts.extend_from_slice(&key.to_key_path().ok()?);
        Some(parts.join("/") + KEYWORDS_RECORD_EXT)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    gh_avatar: String,
    gh_id: i64,
    gh_login: String,
    id: u64,
    name: String,
}

pub const USER_RECORD_EXT: &str = ".json";
pub const USER_TABLE: &str = "table:users";
impl User {
    pub fn to_key(&self) -> Option<String> {
        let name = if self.gh_login.is_empty() {
            self.id.to_string()
        } else {
            self.gh_login.clone()
        };

        assert!(!name.contains('/'));

        let mut parts = vec![USER_TABLE.to_string()];
        let key: dba::Str = name.into();
        parts.extend_from_slice(&key.to_key_path().ok()?);
        Some(parts.join("/") + USER_RECORD_EXT)
    }
}

/// Secondary table, table:versions
#[derive(Debug, Serialize, Deserialize)]
pub struct Version {
    crate_id: u64,
    crate_size: Option<usize>,
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    created_at: String,
    downloads: usize,
    features: String,
    id: u64,
    license: String,
    num: String,
    published_by: Option<u64>,
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    updated_at: String,
    yanked: char,
}

impl Version {
    #[allow(dead_code)] // TODO
    pub fn to_features(&self) -> Result<serde_json::Value> {
        err_at!(FailConvert, serde_json::from_str(&self.features))
    }
}

/// Secondary table, table:badges
#[derive(Debug, Serialize, Deserialize)]
pub struct Badge {
    attributes: String,
    badge_type: String,
    crate_id: u64,
}

impl Badge {
    #[allow(dead_code)] // TODO
    pub fn to_attributes(&self) -> Result<serde_json::Value> {
        err_at!(FailConvert, serde_json::from_str(&self.attributes))
    }
}

/// Secondary table, table:metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    total_downloads: usize,
}

/// Secondary table, table:reserved_crate_names
#[derive(Debug, Serialize, Deserialize)]
pub struct ReservedCrateName {
    name: String,
}

/// Secondary table, table:version_downloads
#[derive(Debug, Serialize, Deserialize)]
pub struct VersionDownloads {
    version_id: u64,
    date: String, // data format %Y-%m-%d
    downloads: usize,
}

/// Secondary table, table:crate_owners
#[derive(Debug, Serialize, Deserialize)]
pub struct CrateOwners {
    crate_id: u64,
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    created_at: String,
    created_by: Option<u64>,
    owner_id: u64,
    owner_kind: u64,
}

/// Secondary table, table:crates_categories
#[derive(Debug, Serialize, Deserialize)]
pub struct CrateCategories {
    crate_id: u64,
    category_id: u64,
}

/// Secondary table, table:crates_keywords
#[derive(Debug, Serialize, Deserialize)]
pub struct CrateKeywords {
    crate_id: u64,
    keyword_id: u64,
}
