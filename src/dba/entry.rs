use chrono::TimeZone;

use std::convert::{TryFrom, TryInto};

use crate::{Error, Result};

pub enum Object {
    Oid {
        hash: Vec<u8>,
    },
    Blob {
        hash: Vec<u8>,
        older: Vec<Vec<u8>>,
        value: Vec<u8>,
    },
    Tree {
        hash: Vec<u8>,
        entries: Vec<Entry>,
    },
    Commit {
        hash: Vec<u8>,
        tree: Box<Object>,
        parents: Vec<Vec<u8>>,
        author: User,
        commiter: User,
    },
}

impl From<git2::Oid> for Object {
    fn from(oid: git2::Oid) -> Object {
        Object::Oid {
            hash: oid.as_bytes().to_vec(),
        }
    }
}

impl<'a> From<git2::Blob<'a>> for Object {
    fn from(blob: git2::Blob) -> Object {
        Object::Blob {
            hash: blob.id().as_bytes().to_vec(),
            older: Vec::default(),
            value: blob.content().to_vec(),
        }
    }
}

impl<'a> TryFrom<git2::Tree<'a>> for Object {
    type Error = Error;

    fn try_from(tree: git2::Tree) -> Result<Object> {
        let mut entries = Vec::with_capacity(tree.len());
        for entry in tree.iter() {
            entries.push(entry.try_into()?);
        }

        let tree = Object::Tree {
            hash: tree.id().as_bytes().to_vec(),
            entries,
        };

        Ok(tree)
    }
}

impl<'a> TryFrom<git2::Commit<'a>> for Object {
    type Error = Error;

    fn try_from(commit: git2::Commit) -> Result<Object> {
        let mut parents = Vec::with_capacity(commit.parent_count());
        for i in 0..parents.capacity() {
            parents.push(
                err_at!(FailGitapi, commit.parent_id(i))?
                    .as_bytes()
                    .to_vec(),
            );
        }
        let tree = err_at!(FailGitapi, commit.tree())?;

        let obj = Object::Commit {
            hash: commit.id().as_bytes().to_vec(),
            tree: Object::try_from(tree)?.into(),
            parents,
            author: commit.author().try_into()?,
            commiter: commit.committer().try_into()?,
        };

        Ok(obj)
    }
}

pub struct Entry {
    pub file_mode: i32,
    pub obj_type: Type,
    pub obj_hash: Vec<u8>,
    pub name: String,
}

impl<'a> TryFrom<git2::TreeEntry<'a>> for Entry {
    type Error = Error;

    fn try_from(te: git2::TreeEntry) -> Result<Entry> {
        let name = match te.name() {
            Some(name) => Ok(name),
            None => err_at!(FailGitapi, msg: "missing name for entry"),
        }?
        .to_string();
        let obj_type: Type = match te.kind() {
            Some(obj_type) => obj_type.try_into(),
            None => err_at!(FailGitapi, msg: "missing kind for {}", name),
        }?;

        let entry = Entry {
            file_mode: te.filemode(),
            obj_type,
            obj_hash: te.id().as_bytes().to_vec(),
            name,
        };

        Ok(entry)
    }
}

pub enum Type {
    Blob,
    Tree,
    Commit,
}

impl TryFrom<git2::ObjectType> for Type {
    type Error = Error;

    fn try_from(t: git2::ObjectType) -> Result<Type> {
        match t {
            git2::ObjectType::Blob => Ok(Type::Blob),
            git2::ObjectType::Tree => Ok(Type::Tree),
            git2::ObjectType::Commit => Ok(Type::Commit),
            _ => err_at!(FailGitapi, msg: "object-type {} is invalid", t),
        }
    }
}

pub struct User {
    pub name: String,
    pub email: String,
    pub timestamp: u64, // utc timestamp from epoch.
}

impl<'a> TryFrom<git2::Signature<'a>> for User {
    type Error = Error;

    fn try_from(signt: git2::Signature) -> Result<User> {
        let name = match signt.name() {
            Some(name) => Ok(name),
            None => err_at!(FailGitapi, msg: "missing user name in signature"),
        }?
        .to_string();

        let timestamp = {
            let time = signt.when();
            let ts = chrono::Utc.timestamp(time.seconds(), 0);
            let offset = match time.offset_minutes() {
                minutes if minutes < 0 => chrono::FixedOffset::west(minutes * 60),
                minutes => chrono::FixedOffset::east(minutes * 60),
            };
            err_at!(FailConvert, (ts + offset).timestamp().try_into())?
        };

        let email = match signt.email() {
            Some(email) => Ok(email),
            None => err_at!(FailGitapi, msg: "missing email in signature for {}", name),
        }?
        .to_string();

        let user = User {
            name,
            email,
            timestamp,
        };

        Ok(user)
    }
}
