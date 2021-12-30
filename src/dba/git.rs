use chrono::TimeZone;

use std::convert::{TryFrom, TryInto};

use crate::{dba, Error, Result};

// Convert types from git2 library into dba types.

impl From<git2::Oid> for dba::Object {
    fn from(oid: git2::Oid) -> dba::Object {
        dba::Object::Oid {
            hash: dba::Oid::from_sha1(oid.as_bytes()),
        }
    }
}

impl<'a> From<git2::Blob<'a>> for dba::Object {
    fn from(blob: git2::Blob) -> dba::Object {
        dba::Object::Blob {
            hash: dba::Oid::from_sha1(blob.id().as_bytes()),
            value: blob.content().to_vec(),
        }
    }
}

impl<'a> TryFrom<git2::Tree<'a>> for dba::Object {
    type Error = Error;

    fn try_from(tree: git2::Tree) -> Result<dba::Object> {
        let mut edges = Vec::with_capacity(tree.len());
        for entry in tree.iter() {
            edges.push(entry.try_into()?);
        }

        let tree = dba::Object::Tree {
            hash: dba::Oid::from_sha1(tree.id().as_bytes()),
            edges,
        };

        Ok(tree)
    }
}

impl<'a> TryFrom<git2::Commit<'a>> for dba::Object {
    type Error = Error;

    fn try_from(commit: git2::Commit) -> Result<dba::Object> {
        let mut parents = Vec::with_capacity(commit.parent_count());
        for i in 0..parents.capacity() {
            parents.push(dba::Oid::from_sha1(
                err_at!(FailGitapi, commit.parent_id(i))?.as_bytes(),
            ));
        }
        let tree = err_at!(FailGitapi, commit.tree())?;

        let obj = dba::Object::Commit {
            hash: dba::Oid::from_sha1(commit.id().as_bytes()),
            tree: dba::Object::try_from(tree)?.into(),
            parents,
            author: commit.author().try_into()?,
            committer: commit.committer().try_into()?,
        };

        Ok(obj)
    }
}

impl<'a> TryFrom<git2::Object<'a>> for dba::Object {
    type Error = Error;

    fn try_from(val: git2::Object) -> Result<dba::Object> {
        match val.kind() {
            Some(git2::ObjectType::Blob) => Ok(val.into_blob().unwrap().into()),
            Some(git2::ObjectType::Tree) => val.into_tree().unwrap().try_into(),
            Some(git2::ObjectType::Commit) => val.into_commit().unwrap().try_into(),
            _ => err_at!(FailGitapi, msg: "invalid object type"),
        }
    }
}

impl<'a> TryFrom<git2::TreeEntry<'a>> for dba::Edge {
    type Error = Error;

    fn try_from(te: git2::TreeEntry) -> Result<dba::Edge> {
        let name = match te.name() {
            Some(name) => Ok(name),
            None => err_at!(FailGitapi, msg: "missing name for entry"),
        }?
        .to_string();
        let obj_type: dba::Type = match te.kind() {
            Some(obj_type) => obj_type.try_into(),
            None => err_at!(FailGitapi, msg: "missing kind for {}", name),
        }?;

        let entry = dba::Edge {
            file_mode: te.filemode(),
            obj_type,
            obj_hash: dba::Oid::from_sha1(te.id().as_bytes()),
            name,
        };

        Ok(entry)
    }
}

impl TryFrom<git2::ObjectType> for dba::Type {
    type Error = Error;

    fn try_from(t: git2::ObjectType) -> Result<dba::Type> {
        match t {
            git2::ObjectType::Blob => Ok(dba::Type::Blob),
            git2::ObjectType::Tree => Ok(dba::Type::Tree),
            git2::ObjectType::Commit => Ok(dba::Type::Commit),
            _ => err_at!(FailGitapi, msg: "object-type {} is invalid", t),
        }
    }
}

impl<'a> TryFrom<git2::Signature<'a>> for dba::User {
    type Error = Error;

    fn try_from(signt: git2::Signature) -> Result<dba::User> {
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

        let user = dba::User {
            name,
            email,
            timestamp,
        };

        Ok(user)
    }
}
