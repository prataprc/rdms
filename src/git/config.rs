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
    /// user information
    pub user_name: String,
    pub user_email: String,
    /// Refer to [InitConfig]
    pub init: InitConfig,
    /// Refer to [OpenConfig]
    pub open: OpenConfig,
}

/// Configurable options for initializing a fresh git datastore.
#[derive(Clone, Serialize, Deserialize)]
pub struct InitConfig {
    /// Create a bare repository with no working directory,
    /// ``DEFAULT: false``.
    pub bare: Option<bool>,
    /// Return an error if the repository path appears to already be a git repository,
    /// ``DEFAULT: true``.
    pub no_reinit: Option<bool>,
    /// Refer to [Permissions].
    pub permissions: Option<Permissions>,
    /// repository/index description.
    pub description: String,
}

/// Configurable options for opening an existing git datastore.
#[derive(Clone, Serialize, Deserialize)]
pub struct OpenConfig {
    /// Only open the specified path; don’t walk upward searching,
    /// ``Default: true``.
    pub no_search: Option<bool>,
}
