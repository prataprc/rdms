//! Git repository as key-value index.
//!
//! Each file (aka blob in git parlance) is considered as a {key,value} entry, where
//! value is the content of the file and key is path starting from repository root
//! to the actual file location when it is checked out.
//!
//! There are few criterias in supplying the key:
//!
//! * must be a valid string
//! * must not start with root or drive prefix
//! * must not start with current directory or parent directory.

use git2::{Repository, RepositoryInitMode, RepositoryInitOptions, RepositoryOpenFlags};

use std::{convert::TryInto, file, fs, ops::Bound, ops::RangeBounds, path, time};

use crate::{dba, git, Error, Result};

macro_rules! iter_result {
    ($res:expr) => {{
        match $res {
            Ok(res) => res,
            Err(err) => {
                let prefix = format!("{}:{}", file!(), line!());
                return Some(Err(Error::FailCbor(prefix, format!("{}", err))));
            }
        }
    }};
}

/// Git repository as Key-Value index.
pub struct Index {
    config: git::Config,
    repo: git2::Repository,
}

impl Index {
    /// Create a new git repository to access it as key-value index.
    pub fn create(config: git::Config) -> Result<Index> {
        let mode = match &config.init.permissions {
            None => RepositoryInitMode::SHARED_UMASK,
            Some(git::Permissions::SharedUmask) => RepositoryInitMode::SHARED_UMASK,
            Some(git::Permissions::SharedGroup) => RepositoryInitMode::SHARED_GROUP,
            Some(git::Permissions::SharedAll) => RepositoryInitMode::SHARED_ALL,
        };

        let mut options = RepositoryInitOptions::new();
        options
            .bare(config.init.bare.unwrap_or(false))
            .no_reinit(config.init.no_reinit.unwrap_or(true))
            .no_dotgit_dir(false)
            .mkdir(true)
            .mkpath(true)
            .mode(mode)
            .description(&config.init.description);

        let repo = {
            let loc = config.loc_repo.clone();
            err_at!(Fatal, Repository::init_opts(loc, &options))?
        };

        // initialize a new repository for key-value access.
        let index = Index { config, repo };

        Ok(index)
    }

    /// Open any existing git repository as key-value index. Refer to [git::Config]
    /// for details.
    pub fn open(config: git::Config) -> Result<Index> {
        let mut flags = RepositoryOpenFlags::empty();
        flags.set(
            RepositoryOpenFlags::NO_SEARCH,
            config.open.no_search.unwrap_or(true),
        );

        // initialize a new repository for key-value access.
        let repo = {
            let ceiling_dirs = Vec::<String>::default().into_iter();
            err_at!(
                Fatal,
                Repository::open_ext(config.loc_repo.clone(), flags, ceiling_dirs)
            )?
        };

        let index = Index { config, repo };

        Ok(index)
    }

    /// Close index, leave the repository as is.
    pub fn close(self) -> Result<()> {
        Ok(())
    }

    /// Purge index, remove disk footprint of the repository and its working tree.
    pub fn purge(self) -> Result<()> {
        err_at!(IOError, fs::remove_dir(&self.config.loc_repo))
    }
}

impl Index {
    /// Return the configuration for this git repository. Refer [git::Config]
    /// type for details.
    pub fn as_config(&self) -> &git::Config {
        &self.config
    }

    /// Return number of items in the repository. Note that this is costly call, it
    /// iterates over every entry in the repository.
    pub fn len(&self) -> Result<usize> {
        let count: usize = self.iter()?.map(|_| 1).sum();
        Ok(count)
    }

    /// Same as calling `index.len() == 0`.
    pub fn is_empty(&self) -> bool {
        match self.len() {
            Ok(n) if n == 0 => true,
            Ok(_) => false,
            Err(_) => false,
        }
    }

    fn to_signature(&self, timestamp: &git2::Time) -> Result<git2::Signature> {
        err_at!(
            FailGitapi,
            git2::Signature::new(
                &self.config.user_name,
                &self.config.user_email,
                &timestamp
            )
        )
    }
}

impl Index {
    /// Get the git blob corresponding to the specified key.
    pub fn get<K>(&self, key: K) -> Result<Option<dba::Object>>
    where
        K: Clone + dba::AsKey,
    {
        let key: path::PathBuf = key.to_key_path()?.into_iter().collect();
        let tree = self.get_db_root()?;
        let te = err_at!(FailGitapi, tree.get_path(&key))?;

        let obj = err_at!(FailGitapi, te.to_object(&self.repo))?;
        let obj: dba::Object = match obj.as_blob().cloned() {
            Some(blob) => Ok(blob.into()),
            None => err_at!(NotFound, msg: "key not found {:?}", key),
        }?;

        Ok(Some(obj))
    }

    /// Iter over each entry in repository in string sort order.
    pub fn iter(&self) -> Result<IterLevel> {
        let val = {
            let tree = self.get_db_root()?;
            let comps = vec![];
            IterLevel::forward(&self.repo, "".into(), tree.clone(), &comps)?
        };
        Ok(val)
    }

    /// Iter over each entry in repository, such that each entry's key falls within
    /// the supplied range.
    pub fn range<R, K>(&self, range: R) -> Result<Range>
    where
        R: RangeBounds<K>,
        K: Clone + dba::AsKey,
    {
        let iter = {
            let tree = self.get_db_root()?;
            let comps = Index::key_to_components(range.start_bound())?;
            IterLevel::forward(&self.repo, "".into(), tree.clone(), &comps)?
        };

        // iter.pretty_print("");

        let high = match range.end_bound() {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(end) => Bound::Included(end.to_key_path()?.iter().collect()),
            Bound::Excluded(end) => Bound::Excluded(end.to_key_path()?.iter().collect()),
        };
        Ok(Range { iter, high })
    }

    /// Same as [Index::range] method except in reverse order.
    pub fn reverse<R, K>(&self, range: R) -> Result<Reverse>
    where
        R: RangeBounds<K>,
        K: Clone + dba::AsKey,
    {
        let iter = {
            let tree = self.get_db_root()?;
            let comps = Index::key_to_components(range.end_bound())?;
            IterLevel::reverse(&self.repo, "".into(), tree.clone(), &comps)?
        };

        // iter.pretty_print("");

        let low = match range.start_bound() {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(s) => Bound::Included(s.to_key_path()?.iter().collect()),
            Bound::Excluded(s) => Bound::Excluded(s.to_key_path()?.iter().collect()),
        };
        Ok(Reverse { iter, low })
    }
}

impl Index {
    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: Clone + dba::AsKey,
        V: AsRef<[u8]>,
    {
        let message = format!("inserting {}", key.to_key_path()?.join("/"));
        let iter = vec![git::WriteOp::Ins { key, value }].into_iter();
        self.commit(iter, &message)?;
        Ok(())
    }

    pub fn remove<K>(&mut self, key: K) -> Result<()>
    where
        K: Clone + dba::AsKey,
    {
        let message = format!("removing {}", key.to_key_path()?.join("/"));
        let iter = vec![git::WriteOp::<K, [u8; 0]>::Rem { key }].into_iter();
        self.commit(iter, &message)?;
        Ok(())
    }

    pub fn commit<K, V, I>(&mut self, ops: I, message: &str) -> Result<usize>
    where
        K: dba::AsKey,
        V: AsRef<[u8]>,
        I: Iterator<Item = git::WriteOp<K, V>>,
    {
        let (mut trie, mut n_ops) = (git::Trie::new(), 0);
        for w in ops {
            match w {
                git::WriteOp::Ins { key, value } => {
                    trie.insert(&key.to_key_path()?, value.as_ref())
                }
                git::WriteOp::Rem { key } => trie.remove(&key.to_key_path()?),
            }
            n_ops += 1;
        }

        self.trie_commit(message, trie, n_ops)
    }

    pub fn transaction(&mut self) -> Result<Txn> {
        let txn = Txn {
            index: self,
            trie: git::Trie::new(),
            n_ops: 0,
        };
        Ok(txn)
    }

    pub fn checkout_head(
        &mut self,
        cb: Option<&mut git2::build::CheckoutBuilder>,
    ) -> Result<()> {
        err_at!(FailGitapi, self.repo.checkout_head(cb))
    }
}

impl Index {
    fn trie_commit(&mut self, message: &str, trie: git::Trie, n: usize) -> Result<usize> {
        let mut odb = err_at!(FailGitapi, self.repo.odb())?;
        let root = trie.as_root();
        let tree = self.get_db_root()?;

        let tree = self.do_commit(&mut odb, Some(tree), root)?;

        // actual commit
        let elapsed_from_epoch = {
            let dur = err_at!(Fatal, time::UNIX_EPOCH.elapsed())?;
            git2::Time::new(dur.as_secs() as i64, 0)
        };
        let update_ref = Some("HEAD");
        let author = self.to_signature(&elapsed_from_epoch)?;
        let committer = self.to_signature(&elapsed_from_epoch)?;
        let parent = {
            let refn = err_at!(FailGitapi, self.repo.find_reference("HEAD"))?;
            err_at!(FailGitapi, refn.peel_to_commit())?
        };

        err_at!(
            FailGitapi,
            self.repo.commit(
                update_ref,
                &author,
                &committer,
                message,
                &tree,
                vec![&parent].as_slice(),
            )
        )?;

        Ok(n)
    }

    fn do_commit(
        &self,
        odb: &mut git2::Odb,
        tree: Option<git2::Tree>,
        node: &git::Node,
    ) -> Result<git2::Tree> {
        let mut builder = err_at!(FailGitapi, self.repo.treebuilder(tree.as_ref()))?;

        // iterate over leafs of this node.
        let blob_type = git2::ObjectType::Blob;
        let blob_mode: i32 = git2::FileMode::Blob.into();
        let mut leaf_names = vec![];
        for leaf in node.as_leafs().iter() {
            match leaf {
                git::Op::Ins { comp, value } => {
                    leaf_names.push(comp.as_str());
                    // println!("insert_blob comp:{}", comp);
                    let oid = err_at!(FailGitapi, odb.write(blob_type, value))?;
                    err_at!(FailGitapi, builder.insert(comp, oid, blob_mode))?;
                }
                git::Op::Rem { comp } => err_at!(FailGitapi, builder.remove(comp))?,
            }
        }

        // traverse deeper
        let tree_mode = git2::FileMode::Tree.into();
        for child in node.as_children().iter() {
            let comp = child.as_comp();

            if leaf_names.contains(&comp) {
                err_at!(FailGitapi, msg: "dir and file same name {}", comp)?
            }
            let oid = match err_at!(FailGitapi, builder.get(comp))? {
                Some(te) => match te.kind() {
                    Some(git2::ObjectType::Tree) => {
                        // println!("do_insert tree:{}", comp);
                        let obj = err_at!(FailGitapi, te.to_object(&self.repo))?;
                        let ct = obj.into_tree().unwrap();
                        Some(self.do_commit(odb, Some(ct), child)?.id())
                    }
                    None => {
                        err_at!(FailGitapi, msg: "missing kind")?;
                        None
                    }
                    _ => {
                        err_at!(FailGitapi, msg: "not a directory {}", comp)?;
                        None
                    }
                },
                None => {
                    // println!("do_commit newtree:{}", comp);
                    Some(self.do_commit(odb, None, child)?.id())
                }
            };
            match oid {
                Some(oid) => {
                    let tree = err_at!(FailGitapi, self.repo.find_tree(oid))?;
                    if !tree.is_empty() {
                        err_at!(FailGitapi, builder.insert(comp, oid, tree_mode))?;
                    }
                }
                None => (),
            }
        }

        let oid = err_at!(FailGitapi, builder.write())?;
        let object = err_at!(FailGitapi, self.repo.find_object(oid, None))?;
        Ok(object.into_tree().unwrap())
    }
}

impl Index {
    fn key_to_components<K>(key: Bound<&K>) -> Result<Vec<Bound<String>>>
    where
        K: dba::AsKey,
    {
        let comps = match key {
            Bound::Unbounded => vec![Bound::Unbounded],
            Bound::Included(key) => key
                .to_key_path()?
                .into_iter()
                .map(|s| Bound::Included(s.to_string()))
                .collect(),
            Bound::Excluded(key) => key
                .to_key_path()?
                .into_iter()
                .map(|s| Bound::Excluded(s.to_string()))
                .collect(),
        };

        Ok(comps)
    }

    fn get_db_root(&self) -> Result<git2::Tree> {
        let tree = {
            let refn = err_at!(FailGitapi, self.repo.head())?;
            let commit = err_at!(FailGitapi, refn.peel_to_commit())?;
            err_at!(FailGitapi, commit.tree())?
        };

        let db_root_loc: path::PathBuf = self.config.loc_db.clone().into();
        let obj = match db_root_loc.as_os_str().is_empty() {
            true => tree.as_object().clone(),
            false => err_at!(
                FailGitapi,
                err_at!(FailGitapi, tree.get_path(&db_root_loc))?.to_object(&self.repo)
            )?,
        };

        match obj.into_tree() {
            Ok(tree) => Ok(tree),
            Err(_) => err_at!(FailGitapi, msg: "git repo root not tree-type"),
        }
    }

    // Entres in tree in reverse sort order.
    fn tree_entries(tree: &git2::Tree) -> Vec<git2::TreeEntry<'static>> {
        let mut items: Vec<git2::TreeEntry> = tree
            .iter()
            .filter(|e| e.name().is_some())
            .map(|e| e.to_owned())
            .collect();
        items.sort_by(|a, b| a.name().unwrap().cmp(b.name().unwrap()));
        items
    }

    fn as_kind_tree<'a>(
        repo: &'a git2::Repository,
        te: git2::TreeEntry,
    ) -> Result<(git2::ObjectType, Option<git2::Tree<'a>>)> {
        let kind = te.kind().unwrap();
        let tree = match kind {
            git2::ObjectType::Tree => {
                err_at!(FailGitapi, te.to_object(repo))?.as_tree().cloned()
            }
            _ => None,
        };
        Ok((kind, tree))
    }
}

// iterate from low to high
pub struct Range<'a> {
    iter: IterLevel<'a>,
    high: Bound<path::PathBuf>,
}

impl<'a> Iterator for Range<'a> {
    type Item = Result<dba::Entry<path::PathBuf>>;

    fn next(&mut self) -> Option<Self::Item> {
        let e = match self.iter.next()? {
            Ok(e) => e,
            Err(e) => return Some(Err(e)),
        };
        let ekey: &path::Path = e.as_key();

        match &self.high {
            Bound::Unbounded => Some(Ok(e)),
            Bound::Included(high) if ekey.le(high) => Some(Ok(e)),
            Bound::Included(_) => {
                self.iter.drain_all();
                None
            }
            Bound::Excluded(high) if ekey.lt(high) => Some(Ok(e)),
            Bound::Excluded(_) => {
                self.iter.drain_all();
                None
            }
        }
    }
}

// iterate from high to low
pub struct Reverse<'a> {
    iter: IterLevel<'a>,
    low: Bound<path::PathBuf>,
}

impl<'a> Iterator for Reverse<'a> {
    type Item = Result<dba::Entry<path::PathBuf>>;

    fn next(&mut self) -> Option<Self::Item> {
        let e = match self.iter.next()? {
            Ok(e) => e,
            Err(e) => return Some(Err(e)),
        };
        let ekey: &path::Path = e.as_key();

        // println!("{:?}", ekey);

        match &self.low {
            Bound::Unbounded => Some(Ok(e)),
            Bound::Included(low) if ekey.ge(low) => Some(Ok(e)),
            Bound::Included(_) => {
                self.iter.drain_all();
                None
            }
            Bound::Excluded(low) if ekey.gt(low) => Some(Ok(e)),
            Bound::Excluded(_) => {
                self.iter.drain_all();
                None
            }
        }
    }
}

enum IterEntry<'a> {
    Entry { te: git2::TreeEntry<'static> },
    Dir { iter: IterLevel<'a> },
}

impl<'a> From<git2::TreeEntry<'static>> for IterEntry<'a> {
    fn from(te: git2::TreeEntry<'static>) -> IterEntry {
        IterEntry::Entry { te }
    }
}

impl<'a> From<IterLevel<'a>> for IterEntry<'a> {
    fn from(iter: IterLevel<'a>) -> IterEntry<'a> {
        IterEntry::Dir { iter }
    }
}

impl<'a> IterEntry<'a> {
    fn to_name(&self) -> &str {
        match self {
            IterEntry::Entry { te } => te.name().unwrap(),
            IterEntry::Dir { .. } => "--dir--",
        }
    }
}

pub struct IterLevel<'a> {
    repo: &'a git2::Repository,
    rloc: path::PathBuf,
    items: Vec<IterEntry<'a>>,
    rev: bool,
}

impl<'a> IterLevel<'a> {
    #[allow(dead_code)]
    fn pretty_print(&self, prefix: &str) {
        let names = self
            .items
            .iter()
            .map(|e| e.to_name())
            .collect::<Vec<&str>>();
        println!("{}IterLevel<{:?}> items:{:?}", prefix, self.rloc, names);

        let prefix = prefix.to_string() + "  ";
        if let Some(x) = self.items.first() {
            match x {
                IterEntry::Dir { iter } => iter.pretty_print(&prefix),
                IterEntry::Entry { te } => {
                    println!("{}...@{}", prefix, te.name().unwrap())
                }
            }
        }
    }

    fn forward(
        repo: &'a git2::Repository,
        rloc: path::PathBuf, // following `tree` argument is under `rloc` path.
        tree: git2::Tree,
        comps: &[Bound<String>],
    ) -> Result<IterLevel<'a>> {
        let (items, par) = (Index::tree_entries(&tree), rloc.clone());

        //println!(
        //    "forward rloc:{:?} comps:{:?} items:{:?}",
        //    rloc,
        //    comps,
        //    items.len(),
        //);

        let (item, mut items) = match comps.first() {
            Some(Bound::Unbounded) => (None, items),
            Some(Bound::Included(comp)) => {
                match items.binary_search_by(|e| e.name().unwrap().cmp(comp)) {
                    Ok(off) => match Index::as_kind_tree(repo, items[off].clone())? {
                        (git2::ObjectType::Tree, Some(nt)) => {
                            let subdir = items[off].name().unwrap().into();
                            let rloc: path::PathBuf = [par, subdir].iter().collect();
                            let level = Self::forward(repo, rloc, nt, &comps[1..])?;
                            (Some(level), items[(off + 1)..].to_vec())
                        }
                        (git2::ObjectType::Blob, _) => (None, items[off..].to_vec()),
                        (_, _) => unreachable!(),
                    },
                    Err(off) => (None, items[off..].to_vec()),
                }
            }
            Some(Bound::Excluded(comp)) => {
                match items.binary_search_by(|e| e.name().unwrap().cmp(comp)) {
                    Ok(off) => match Index::as_kind_tree(repo, items[off].clone())? {
                        (git2::ObjectType::Tree, Some(nt)) => {
                            let subdir = items[off].name().unwrap().into();
                            let rloc: path::PathBuf = [par, subdir].iter().collect();
                            let level = Self::forward(repo, rloc, nt, &comps[1..])?;
                            (Some(level), items[(off + 1)..].to_vec())
                        }
                        (git2::ObjectType::Blob, _) => (None, items[off + 1..].to_vec()),
                        (_, _) => unreachable!(),
                    },
                    Err(off) => (None, items[off..].to_vec()),
                }
            }
            None => (None, items),
        };

        items.reverse();

        let mut items: Vec<IterEntry> = items.into_iter().map(IterEntry::from).collect();
        if let Some(item) = item {
            items.push(IterEntry::from(item))
        }

        let val = IterLevel {
            repo,
            rloc,
            items,
            rev: false,
        };

        Ok(val)
    }

    fn reverse(
        repo: &'a git2::Repository,
        rloc: path::PathBuf, // following `tree` argument is under `rloc` path.
        tree: git2::Tree,
        comps: &[Bound<String>],
    ) -> Result<IterLevel<'a>> {
        let (mut items, par) = (Index::tree_entries(&tree), rloc.clone());
        items.reverse();

        //println!(
        //    "reverse rloc:{:?} comps:{:?} items:{:?}",
        //    rloc,
        //    comps,
        //    items.len(),
        //);

        let (item, mut items) = match comps.first() {
            Some(Bound::Unbounded) => (None, items),
            Some(Bound::Included(comp)) => {
                match items.binary_search_by(|e| e.name().unwrap().cmp(comp).reverse()) {
                    Ok(off) => match Index::as_kind_tree(repo, items[off].clone())? {
                        (git2::ObjectType::Tree, Some(nt)) => {
                            let subdir = items[off].name().unwrap().into();
                            let rloc: path::PathBuf = [par, subdir].iter().collect();
                            let level = Self::reverse(repo, rloc, nt, &comps[1..])?;
                            (Some(level), items[(off + 1)..].to_vec())
                        }
                        (git2::ObjectType::Blob, _) => (None, items[off..].to_vec()),
                        (_, _) => unreachable!(),
                    },
                    Err(off) => (None, items[off..].to_vec()),
                }
            }
            Some(Bound::Excluded(comp)) => {
                match items.binary_search_by(|e| e.name().unwrap().cmp(comp).reverse()) {
                    Ok(off) => match Index::as_kind_tree(repo, items[off].clone())? {
                        (git2::ObjectType::Tree, Some(nt)) => {
                            let subdir = items[off].name().unwrap().into();
                            let rloc: path::PathBuf = [par, subdir].iter().collect();
                            let level = Self::reverse(repo, rloc, nt, &comps[1..])?;
                            (Some(level), items[(off + 1)..].to_vec())
                        }
                        (git2::ObjectType::Blob, _) => (None, items[off + 1..].to_vec()),
                        (_, _) => unreachable!(),
                    },
                    Err(off) => (None, items[off..].to_vec()),
                }
            }
            None => (None, items),
        };

        items.reverse();

        let mut items: Vec<IterEntry> = items.into_iter().map(IterEntry::from).collect();
        if let Some(item) = item {
            items.push(IterEntry::from(item))
        };

        let val = IterLevel {
            repo,
            rloc,
            items,
            rev: true,
        };

        Ok(val)
    }

    fn drain_all(&mut self) {
        self.items.drain(..);
    }
}

impl<'a> Iterator for IterLevel<'a> {
    type Item = Result<dba::Entry<path::PathBuf>>;

    fn next(&mut self) -> Option<Self::Item> {
        // println!("iterlevel {:?} ref:{}", self.rloc, self.rev);

        loop {
            let item = self.items.pop()?;
            match item {
                IterEntry::Entry { te } => match te.kind().unwrap() {
                    git2::ObjectType::Blob => {
                        // println!("{} iterl-blob {}", n, te.name().unwrap());
                        let entry =
                            iter_result!(te_to_entry(self.repo, self.rloc.clone(), te));
                        break Some(Ok(entry));
                    }
                    git2::ObjectType::Tree => {
                        // println!("{} iterl-tree {}", n, te.name().unwrap());
                        let tree = {
                            let val = iter_result!(te.to_object(self.repo));
                            val.into_tree().unwrap().clone()
                        };
                        let rloc: path::PathBuf =
                            [self.rloc.clone(), te.name().unwrap().into()]
                                .iter()
                                .collect();

                        let iter = match self.rev {
                            false => iter_result!(IterLevel::forward(
                                self.repo,
                                rloc,
                                tree,
                                &[] // comps
                            )),
                            true => iter_result!(IterLevel::reverse(
                                self.repo,
                                rloc,
                                tree,
                                &[] // comps
                            )),
                        };
                        // iter.pretty_print("...");
                        self.items.push(iter.into());
                        break self.next();
                    }
                    _ => unreachable!(),
                },
                IterEntry::Dir { mut iter } => {
                    if let Some(entry) = iter.next() {
                        // println!("{} iterl-dir {:?}", n, entry.as_ref().unwrap().key);
                        self.items.push(iter.into());
                        break Some(entry);
                    }
                }
            }
        }
    }
}

pub struct Txn<'a> {
    index: &'a mut Index,
    trie: git::Trie,
    n_ops: usize,
}

impl<'a> Txn<'a> {
    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: dba::AsKey,
        V: AsRef<[u8]>,
    {
        self.trie.insert(&key.to_key_path()?, value.as_ref());
        self.n_ops += 1;
        Ok(())
    }

    pub fn remove<K>(&mut self, key: K) -> Result<()>
    where
        K: dba::AsKey,
    {
        self.trie.remove(&key.to_key_path()?);
        self.n_ops += 1;
        Ok(())
    }

    pub fn commit(self, message: &str) -> Result<usize> {
        self.index.trie_commit(message, self.trie, self.n_ops)
    }
}

fn te_to_entry(
    repo: &git2::Repository,
    mut rloc: path::PathBuf,
    te: git2::TreeEntry<'static>,
) -> Result<dba::Entry<path::PathBuf>> {
    let key: path::PathBuf = {
        rloc.push(te.name().unwrap());
        rloc.into()
    };

    let obj: dba::Object = err_at!(FailGitapi, te.to_object(repo))?.try_into()?;

    let entry = dba::Entry::from_object(key, obj);
    Ok(entry)
}
