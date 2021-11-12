//! Configuration information to open a git repository as key-value index.

use serde::{Deserialize, Serialize};

/// Permission to use while creating repository
#[derive(Clone, Serialize, Deserialize)]
pub enum Permissions {
    #[serde(rename = "shared_umask")]
    SharedUmask,
    #[serde(rename = "shared_group")]
    SharedGroup,
    #[serde(rename = "shared_all")]
    SharedAll,
}

/// Configuration describing the index backed by a _git-repository_.
#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    /// location of repository root.
    pub loc_repo: String,
    /// location of database keys, aka file-names, relative to root.
    pub loc_db: String,
    /// repository permissions.
    pub permissions: Option<Permissions>,
    /// repository/index description.
    pub description: String,
}
