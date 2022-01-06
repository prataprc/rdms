//! Configuration information to open a git repository as key-value index.

use serde::{Deserialize, Serialize};

/// Type define permissions to create/access git-repository. Maps to git permissions.
#[derive(Clone, Serialize, Deserialize)]
pub enum Permissions {
    #[serde(rename = "shared_umask")]
    SharedUmask,
    #[serde(rename = "shared_group")]
    SharedGroup,
    #[serde(rename = "shared_all")]
    SharedAll,
}

/// Type to configure _git-repository_ while creating them and opening them.
#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    /// location of repository root.
    pub loc_repo: String,
    /// location of database keys, aka file-names, relative to root.
    pub loc_db: String,
    /// user information to be used in git-commits.
    pub user_name: String,
    /// user information to be used in git-commits.
    pub user_email: String,
    /// Refer to [InitConfig]
    pub init: InitConfig,
    /// Refer to [OpenConfig]
    pub open: OpenConfig,
}

/// Type to configure _git-repository_ while creating them.
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

/// Type to configure _git-repository_ while opening them.
#[derive(Clone, Serialize, Deserialize)]
pub struct OpenConfig {
    /// Only open the specified path; don’t walk upward searching,
    /// ``Default: true``.
    pub no_search: Option<bool>,
}
