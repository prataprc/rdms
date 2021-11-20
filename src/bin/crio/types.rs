use serde::{Deserialize, Serialize};

use rdms::{dba, err_at, Error, Result};

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
pub const CRATE_KEY_LEVELS: usize = 3;
impl Crate {
    pub fn to_key(&self) -> Option<String> {
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Badge {
    attributes: String,
    badge_type: String,
    crate_id: u64,
}

pub const BADGE_RECORD_EXT: &str = ".json";
pub const BADGE_TABLE: &str = "table:badges";
pub const BADGE_KEY_LEVELS: usize = 3;
impl Badge {
    pub fn to_key(&self) -> Option<String> {
        let name = self.crate_id.to_string();
        assert!(!name.contains('/'));

        let mut full_key = BADGE_TABLE.to_string();

        match dba::to_content_key(&name, BADGE_KEY_LEVELS) {
            Some(key) => {
                full_key.push('/');
                full_key.push_str(&key);
                full_key.push('/');
                full_key.push_str(&self.badge_type);
                full_key.push_str(BADGE_RECORD_EXT);
                Some(full_key)
            }
            None => None,
        }
    }

    #[allow(dead_code)] // TODO
    pub fn to_attributes(&self) -> Result<serde_json::Value> {
        err_at!(FailConvert, serde_json::from_str(&self.attributes))
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
pub const KEYWORDS_KEY_LEVELS: usize = 3;
impl Keyword {
    pub fn to_key(&self) -> Option<String> {
        assert!(!self.keyword.contains('/'));

        let mut full_key = KEYWORDS_TABLE.to_string();

        match dba::to_content_key(&self.keyword, KEYWORDS_KEY_LEVELS) {
            Some(key) => {
                full_key.push('/');
                full_key.push_str(&key);
                full_key.push_str(KEYWORDS_RECORD_EXT);
                Some(full_key)
            }
            None => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    total_downloads: usize,
}

impl Metadata {
    pub fn to_key(&self) -> Option<String> {
        Some("metadata.json".to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReservedCrateName {
    name: String,
}

pub const RESERVED_CRATE_NAME_RECORD_EXT: &str = ".json";
pub const RESERVED_CRATE_NAME_TABLE: &str = "table:reserved_crate_names";
impl ReservedCrateName {
    pub fn to_key(&self) -> Option<String> {
        assert!(!self.name.contains('/'));

        let mut full_key = RESERVED_CRATE_NAME_TABLE.to_string();

        full_key.push('/');
        full_key.push_str(&self.name);
        full_key.push_str(RESERVED_CRATE_NAME_RECORD_EXT);

        Some(full_key)
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
pub const USER_KEY_LEVELS: usize = 3;
impl User {
    pub fn to_key(&self) -> Option<String> {
        let name = if self.gh_login.is_empty() {
            self.id.to_string()
        } else {
            self.gh_login.clone()
        };
        assert!(!name.contains('/'));

        let mut full_key = USER_TABLE.to_string();

        match dba::to_content_key(&name, USER_KEY_LEVELS) {
            Some(key) => {
                full_key.push('/');
                full_key.push_str(&key);
                full_key.push_str(USER_RECORD_EXT);
                Some(full_key)
            }
            None => None,
        }
    }
}

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

pub const VERSION_RECORD_EXT: &str = ".json";
pub const VERSION_TABLE: &str = "table:versions";
pub const VERSION_KEY_LEVELS: usize = 3;
impl Version {
    pub fn to_key(&self) -> Option<String> {
        let name = self.crate_id.to_string();
        assert!(!name.contains('/'));

        let mut full_key = VERSION_TABLE.to_string();

        match dba::to_content_key(&name, VERSION_KEY_LEVELS) {
            Some(key) => {
                full_key.push('/');
                full_key.push_str(&key);
                full_key.push('/');
                full_key.push_str(&self.num);
                full_key.push_str(VERSION_RECORD_EXT);
                Some(full_key)
            }
            None => None,
        }
    }

    #[allow(dead_code)] // TODO
    pub fn to_features(&self) -> Result<serde_json::Value> {
        err_at!(FailConvert, serde_json::from_str(&self.features))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionDownloads {
    version_id: u64,
    date: String, // data format %Y-%m-%d
    downloads: usize,
}

pub const VERSION_DOWNLOADS_RECORD_EXT: &str = ".json";
pub const VERSION_DOWNLOADS_TABLE: &str = "table:version_downloads";
impl VersionDownloads {
    pub fn to_key(&self) -> Option<String> {
        let name = self.version_id.to_string();
        assert!(!name.contains('/'));

        let mut full_key = VERSION_DOWNLOADS_TABLE.to_string();

        match dba::to_content_key(&name, VERSION_KEY_LEVELS) {
            Some(key) => {
                full_key.push('/');
                full_key.push_str(&key);
                full_key.push('/');
                full_key.push_str(&self.date);
                full_key.push_str(VERSION_DOWNLOADS_RECORD_EXT);
                Some(full_key)
            }
            None => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrateOwners {
    crate_id: u64,
    // data/time format %Y-%m-%d %H:%M:%S.6f TODO is this UTC timezone ?
    created_at: String,
    created_by: Option<u64>,
    owner_id: u64,
    owner_kind: u64,
}

pub const CRATE_OWNERS_RECORD_EXT: &str = ".json";
pub const CRATE_OWNERS_TABLE: &str = "table:crate_owners";
pub const CRATE_OWNERS_KEY_LEVELS: usize = 3;
impl CrateOwners {
    pub fn to_key(&self) -> Option<String> {
        let name = self.crate_id.to_string();
        assert!(!name.contains('/'));

        let mut full_key = CRATE_OWNERS_TABLE.to_string();
        //let owner_id = self
        //    .owner_id
        //    .as_ref()
        //    .map(|x| x.to_string())
        //    .unwrap_or("anonymous".to_string());

        match dba::to_content_key(&name, CRATE_OWNERS_KEY_LEVELS) {
            Some(key) => {
                full_key.push('/');
                full_key.push_str(&key);
                full_key.push('/');
                full_key.push_str(&self.owner_id.to_string());
                full_key.push_str(CRATE_OWNERS_RECORD_EXT);
                Some(full_key)
            }
            None => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrateCategories {
    crate_id: u64,
    category_id: u64,
}

pub const CRATE_CATEGORIES_RECORD_EXT: &str = ".json";
pub const CRATE_CATEGORIES_TABLE: &str = "table:crate_categories";
pub const CRATE_CATEGORIES_KEY_LEVELS: usize = 3;
impl CrateCategories {
    pub fn to_key(&self) -> Option<String> {
        let name = self.crate_id.to_string();
        assert!(!name.contains('/'));

        let mut full_key = CRATE_CATEGORIES_TABLE.to_string();

        match dba::to_content_key(&name, CRATE_CATEGORIES_KEY_LEVELS) {
            Some(key) => {
                full_key.push('/');
                full_key.push_str(&key);
                full_key.push('/');
                full_key.push_str(&self.category_id.to_string());
                full_key.push_str(CRATE_CATEGORIES_RECORD_EXT);
                Some(full_key)
            }
            None => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrateKeywords {
    crate_id: u64,
    keyword_id: u64,
}

pub const CRATE_KEYWORDS_RECORD_EXT: &str = ".json";
pub const CRATE_KEYWORDS_TABLE: &str = "table:crate_keywords";
pub const CRATE_KEYWORDS_KEY_LEVELS: usize = 3;
impl CrateKeywords {
    pub fn to_key(&self) -> Option<String> {
        let name = self.crate_id.to_string();
        assert!(!name.contains('/'));

        let mut full_key = CRATE_KEYWORDS_TABLE.to_string();

        match dba::to_content_key(&name, CRATE_KEYWORDS_KEY_LEVELS) {
            Some(key) => {
                full_key.push('/');
                full_key.push_str(&key);
                full_key.push('/');
                full_key.push_str(&self.keyword_id.to_string());
                full_key.push_str(CRATE_KEYWORDS_RECORD_EXT);
                Some(full_key)
            }
            None => None,
        }
    }
}
