use serde::{Deserialize, Serialize};

use std::ffi;

#[derive(Serialize, Deserialize)]
pub enum Permissions {
    #[serde(rename = "shared_umask")]
    SharedUmask,
    #[serde(rename = "shared_group")]
    SharedGroup,
    #[serde(rename = "shared_all")]
    SharedAll,
}

/// Configuration describing the index backed by a _git-repository_.
#[derive(Serialize, Deserialize)]
pub struct Config {
    /// location of repository root.
    pub loc_repo: ffi::OsString,
    /// location of database keys, aka file-names, relative to root.
    pub loc_db: ffi::OsString,
    /// repository permissions.
    pub permissions: Option<Permissions>,
    /// repository/index description.
    pub description: String,
}
