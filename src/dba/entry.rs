use std::{fmt, result};

use crate::dba;

/// Type is object id, which is a hash digest of object's content.
#[derive(Clone)]
pub enum Oid {
    Sha1 { hash: [u8; 20] },
}

impl Oid {
    /// Create a new Oid from digest.
    pub fn from_sha1(digest: &[u8]) -> Oid {
        let mut hash = [0; 20];
        hash[..].copy_from_slice(digest);
        Oid::Sha1 { hash }
    }

    /// Return the raw-bytes of sha1 hash.
    pub fn as_sha1(&self) -> Option<&[u8]> {
        match self {
            Oid::Sha1 { hash } => Some(hash),
        }
    }
}

/// Type of an object.
#[derive(Clone)]
pub enum Type {
    /// Object is a binary blob, a leaf node in the commit-tree.
    Blob,
    /// Object is sub-tree in the commit-tree. Holds onto a collection of other objects.
    Tree,
    /// A commit object, aka root object from which entire merkel-tree was constructed.
    Commit,
}

/// Type forms the core of DBA storage design.
#[derive(Clone)]
pub enum Object {
    /// A leaf object is called `blob`, it just a binary-blob of data.
    Blob { hash: Oid, value: Vec<u8> },
    /// An intermediate object, holds onto a collection of other objects.
    Tree { hash: Oid, edges: Vec<Edge> },
    /// A root object from which entire merkel-tree was constructed.
    Commit {
        hash: Oid,
        tree: Box<Object>,
        parents: Vec<Oid>,
        author: User,
        committer: User,
    },
    /// Holds a reference to an actual object.
    Oid { hash: Oid },
}

/// Type represents a connection between parent node and one of its child node in
/// the merkel-tree.
#[derive(Clone)]
pub struct Edge {
    pub file_mode: i32,
    pub obj_type: Type,
    pub obj_hash: Oid,
    pub name: String,
}

impl Object {
    /// Return object's Oid, its hash-digest.
    pub fn as_oid(&self) -> &Oid {
        match self {
            Object::Oid { hash } => hash,
            Object::Blob { hash, .. } => hash,
            Object::Tree { hash, .. } => hash,
            Object::Commit { hash, .. } => hash,
        }
    }

    /// If object is a blob, return the content of the object.
    pub fn as_content(&self) -> Option<&[u8]> {
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

/// Type define user-detail needed to create a commit object.
#[derive(Clone)]
pub struct User {
    pub name: String,
    pub email: String,
    pub timestamp: u64, // utc timestamp from epoch.
}

/// Type define a single entry in a DBA storage.
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

    pub fn as_oid(&self) -> &dba::Oid {
        self.obj.as_oid()
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
