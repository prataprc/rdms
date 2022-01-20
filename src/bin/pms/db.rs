use std::path;

use rdms::git;

pub struct Project {
    key: path::PathBuf,
    vcs: Vcs,
}

#[derive(Clone)]
pub enum Type {
    Local,
    Source,
    Fork,
    Archive,
    Public,
    Private,
}

#[derive(Clone)]
pub enum RemoteSoT {
    Github,
    Gitlab,
}

#[derive(Clone)]
pub enum Vcs {
    Git { repo: git::Repo },
}
