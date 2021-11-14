use std::{fmt, result};

use crate::dba;

#[derive(Clone)]
pub enum Object {
    Oid {
        hash: Oid,
    },
    Blob {
        hash: Oid,
        older: Vec<Oid>,
        value: Vec<u8>,
    },
    Tree {
        hash: Oid,
        edges: Vec<Edge>,
    },
    Commit {
        hash: Oid,
        tree: Box<Object>,
        parents: Vec<Oid>,
        author: User,
        committer: User,
    },
}

impl Object {
    pub fn to_oid(&self) -> Oid {
        match self {
            Object::Oid { hash } => hash.clone(),
            Object::Blob { hash, .. } => hash.clone(),
            Object::Tree { hash, .. } => hash.clone(),
            Object::Commit { hash, .. } => hash.clone(),
        }
    }

    pub fn as_value(&self) -> Option<&[u8]> {
        match self {
            Object::Blob { value, .. } => Some(value),
            _ => None,
        }
    }

    pub fn iter_edges(&self) -> Option<impl Iterator<Item = &Edge>> {
        match self {
            Object::Tree { edges, .. } => Some(edges.iter()),
            _ => None,
        }
    }

    pub fn as_tree(&self) -> Option<&Object> {
        match self {
            Object::Commit { tree, .. } => Some(tree),
            _ => None,
        }
    }

    pub fn iter_parents(&self) -> Option<impl Iterator<Item = &Oid>> {
        match self {
            Object::Commit { parents, .. } => Some(parents.iter()),
            _ => None,
        }
    }

    pub fn as_author(&self) -> Option<&User> {
        match self {
            Object::Commit { author, .. } => Some(author),
            _ => None,
        }
    }

    pub fn as_committer(&self) -> Option<&User> {
        match self {
            Object::Commit { committer, .. } => Some(committer),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct Edge {
    pub file_mode: i32,
    pub obj_type: Type,
    pub obj_hash: Oid,
    pub name: String,
}

#[derive(Clone)]
pub enum Type {
    Blob,
    Tree,
    Commit,
}

#[derive(Clone)]
pub struct User {
    pub name: String,
    pub email: String,
    pub timestamp: u64, // utc timestamp from epoch.
}

#[derive(Clone)]
pub enum Oid {
    Sha1 { hash: [u8; 20] },
}

impl Oid {
    pub fn from_sha1(bytes: &[u8]) -> Oid {
        let mut hash = [0; 20];
        hash[..].copy_from_slice(bytes);
        Oid::Sha1 { hash }
    }

    pub fn to_shah1(&self) -> &[u8] {
        match self {
            Oid::Sha1 { hash } => hash,
        }
    }
}

#[derive(Clone)]
pub struct Entry<K>
where
    K: dba::AsKey,
{
    key: K,
    obj: dba::Object,
}

impl<K> Entry<K>
where
    K: dba::AsKey,
{
    pub fn from_object(key: K, obj: dba::Object) -> Entry<K> {
        Entry { key, obj }
    }
}

impl<K> Entry<K>
where
    K: dba::AsKey,
{
    pub fn as_key(&self) -> &K {
        &self.key
    }

    pub fn as_obj(&self) -> &dba::Object {
        &self.obj
    }
}

impl<K> fmt::Display for Entry<K>
where
    K: dba::AsKey + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Entry<{}>", self.key)
    }
}

impl<K> fmt::Debug for Entry<K>
where
    K: dba::AsKey + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Entry<{:?}>", self.key)
    }
}
