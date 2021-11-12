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

use std::{file, fmt, fs, ops::Bound, ops::RangeBounds, path, result, time};

use crate::{
    git::{Config, Permissions},
    Error, Result,
};

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
    config: Config,
    repo: git2::Repository,
}

impl Index {
    /// Create a new git repository to access it as key-value index.
    pub fn create(config: Config) -> Result<Index> {
        let mode = match &config.permissions {
            None => RepositoryInitMode::SHARED_UMASK,
            Some(Permissions::SharedUmask) => RepositoryInitMode::SHARED_UMASK,
            Some(Permissions::SharedGroup) => RepositoryInitMode::SHARED_GROUP,
            Some(Permissions::SharedAll) => RepositoryInitMode::SHARED_ALL,
        };

        let mut options = RepositoryInitOptions::new();
        options
            .bare(false)
            .no_reinit(true)
            .no_dotgit_dir(false)
            .mkdir(true)
            .mkpath(true)
            .mode(mode)
            .description(&config.description);

        let repo = {
            let loc = config.loc_repo.clone();
            err_at!(Fatal, Repository::init_opts(loc, &options))?
        };

        // initialize a new repository for key-value access.
        let index = Index { config, repo };

        Ok(index)
    }

    /// Open any existing git repository as key-value index. Refer to [Config] for details.
    pub fn open(config: Config) -> Result<Index> {
        let mut flags = RepositoryOpenFlags::empty();
        flags.set(RepositoryOpenFlags::NO_SEARCH, true);

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
    /// Return the configuration for this git repository. Refer [Config] type for
    /// details.
    pub fn as_config(&self) -> &Config {
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
}

impl Index {
    /// Get the git blob corresponding to the specified key.
    pub fn get<P>(&self, key: P) -> Result<Option<Entry>>
    where
        P: Clone + AsRef<path::Path>,
    {
        let key: &path::Path = key.as_ref();
        let tree = self.get_db_root()?.into_tree().unwrap();
        let te = err_at!(FailGitapi, tree.get_path(key))?;

        let data = {
            let obj = err_at!(FailGitapi, te.to_object(&self.repo))?;
            obj.as_blob().unwrap().content().to_vec()
        };

        let entry = Entry {
            key: key.into(),
            data,
        };

        Ok(Some(entry))
    }

    /// Iter over each entry in repository in string sort order.
    pub fn iter(&self) -> Result<IterLevel> {
        let val = {
            let tree = self.get_db_root()?.into_tree().unwrap();
            let comps = vec![];
            IterLevel::forward(&self.repo, "".into(), tree.clone(), &comps)?
        };
        Ok(val)
    }

    /// Iter over each entry in repository, such that each entry's key falls within
    /// the supplied range.
    pub fn range<R, P>(&self, range: R) -> Result<Range<P>>
    where
        R: RangeBounds<P>,
        P: Clone + AsRef<path::Path>,
    {
        let iter = {
            let tree = self.get_db_root()?.into_tree().unwrap();
            let comps = Index::key_to_components(range.start_bound());
            IterLevel::forward(&self.repo, "".into(), tree.clone(), &comps)?
        };

        // iter.pretty_print("");

        let high = range.end_bound().cloned();
        Ok(Range { iter, high })
    }

    /// Same as [Index::range] method except in reverse order.
    pub fn reverse<R, P>(&self, range: R) -> Result<Reverse<P>>
    where
        R: RangeBounds<P>,
        P: Clone + AsRef<path::Path>,
    {
        let iter = {
            let tree = self.get_db_root()?.into_tree().unwrap();
            let comps = Index::key_to_components(range.end_bound());
            IterLevel::reverse(&self.repo, "".into(), tree.clone(), &comps)?
        };

        // iter.pretty_print("");

        let low = range.start_bound().cloned();
        Ok(Reverse { iter, low })
    }
}

impl Index {
    pub fn insert<P, V>(&mut self, key: P, value: V) -> Result<()>
    where
        P: AsRef<path::Path>,
        V: AsRef<[u8]>,
    {
        let mut txn = self.transaction()?;
        txn.insert(key, value)?;
        txn.commit()
    }

    pub fn remove<P>(&mut self, key: P) -> Result<()>
    where
        P: AsRef<path::Path>,
    {
        let mut txn = self.transaction()?;
        txn.remove(key)?;
        txn.commit()
    }

    pub fn transaction(&mut self) -> Result<Txn> {
        let tree = self.get_db_root()?.into_tree().unwrap();
        let txn = Txn { index: self, tree };
        Ok(txn)
    }
}

impl Index {
    fn key_to_components<P>(key: Bound<P>) -> Vec<Bound<String>>
    where
        P: AsRef<path::Path>,
    {
        match key {
            Bound::Unbounded => vec![Bound::Unbounded],
            Bound::Included(key) => {
                let key: &path::Path = key.as_ref();
                key.components()
                    .filter_map(|c| match c {
                        path::Component::Normal(s) => {
                            Some(Bound::Included(s.to_str()?.to_string()))
                        }
                        _ => None,
                    })
                    .collect()
            }
            Bound::Excluded(key) => {
                let key: &path::Path = key.as_ref();
                key.components()
                    .filter_map(|c| match c {
                        path::Component::Normal(s) => {
                            Some(Bound::Excluded(s.to_str()?.to_string()))
                        }
                        _ => None,
                    })
                    .collect()
            }
        }
    }

    fn get_db_root(&self) -> Result<git2::Object> {
        let tree = {
            let refn = err_at!(FailGitapi, self.repo.head())?;
            let commit = err_at!(FailGitapi, refn.peel_to_commit())?;
            err_at!(FailGitapi, commit.tree())?
        };

        let db_path = {
            let repo_path = path::Path::new(&self.config.loc_repo);
            let db_path = path::Path::new(&self.config.loc_db);
            err_at!(InvalidInput, db_path.strip_prefix(&repo_path))?
        };

        let obj = match db_path.as_os_str().is_empty() {
            true => tree.as_object().clone(),
            false => err_at!(
                FailGitapi,
                err_at!(FailGitapi, tree.get_path(db_path))?.to_object(&self.repo)
            )?,
        };

        Ok(obj)
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
pub struct Range<'a, P>
where
    P: AsRef<path::Path>,
{
    iter: IterLevel<'a>,
    high: Bound<P>,
}

impl<'a, P> Iterator for Range<'a, P>
where
    P: AsRef<path::Path>,
{
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        let e = match self.iter.next()? {
            Ok(e) => e,
            Err(e) => return Some(Err(e)),
        };
        let ekey: &path::Path = e.key.as_ref();

        match &self.high {
            Bound::Unbounded => Some(Ok(e)),
            Bound::Included(p) if ekey.le(p.as_ref()) => Some(Ok(e)),
            Bound::Included(_) => {
                self.iter.drain_all();
                None
            }
            Bound::Excluded(p) if ekey.lt(p.as_ref()) => Some(Ok(e)),
            Bound::Excluded(_) => {
                self.iter.drain_all();
                None
            }
        }
    }
}

// iterate from high to low
pub struct Reverse<'a, P>
where
    P: AsRef<path::Path>,
{
    iter: IterLevel<'a>,
    low: Bound<P>,
}

impl<'a, P> Iterator for Reverse<'a, P>
where
    P: AsRef<path::Path>,
{
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        let e = match self.iter.next()? {
            Ok(e) => e,
            Err(e) => return Some(Err(e)),
        };
        let ekey: &path::Path = e.key.as_ref();
        // println!("{:?}", ekey);

        match &self.low {
            Bound::Unbounded => Some(Ok(e)),
            Bound::Included(p) if ekey.ge(p.as_ref()) => Some(Ok(e)),
            Bound::Included(_) => {
                self.iter.drain_all();
                None
            }
            Bound::Excluded(p) if ekey.gt(p.as_ref()) => Some(Ok(e)),
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
    #[allow(dead_code)]
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
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        // println!("iterlevel {:?} ref:{}", self.rloc, self.rev);

        loop {
            let item = self.items.pop()?;
            match item {
                IterEntry::Entry { te } => match te.kind().unwrap() {
                    git2::ObjectType::Blob => {
                        // println!("{} iterl-blob {}", n, te.name().unwrap());
                        let entry = iter_result!(Entry::from_tree_entry(
                            self.repo,
                            self.rloc.clone(),
                            te
                        ));
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
    index: &'a Index,
    tree: git2::Tree<'a>,
}

impl<'a> Txn<'a> {
    pub fn commit(&mut self) -> Result<()> {
        let elapsed_from_epoch = {
            let dur = err_at!(Fatal, time::UNIX_EPOCH.elapsed())?;
            git2::Time::new(dur.as_secs() as i64, 0)
        };
        let update_ref = Some("HEAD");
        let author = err_at!(
            FailGitapi,
            git2::Signature::new("rdms/git", "no-email-id", &elapsed_from_epoch)
        )?;
        let committer = err_at!(
            FailGitapi,
            git2::Signature::new("rdms/git", "no-email-id", &elapsed_from_epoch)
        )?;
        let message = "dummy message".to_string();
        let parent = {
            let refn = err_at!(FailGitapi, self.index.repo.find_reference("HEAD"))?;
            err_at!(FailGitapi, refn.peel_to_commit())?
        };

        err_at!(
            FailGitapi,
            self.index.repo.commit(
                update_ref,
                &author,
                &committer,
                &message,
                &self.tree,
                vec![&parent].as_slice(),
            )
        )?;

        Ok(())
    }

    pub fn insert<P, V>(&mut self, key: P, data: V) -> Result<()>
    where
        P: AsRef<path::Path>,
        V: AsRef<[u8]>,
    {
        let key: &path::Path = key.as_ref();

        let comps: Vec<String> = key
            .components()
            .filter_map(|c| match c {
                path::Component::Normal(s) => Some(s.to_str()?.to_string()),
                _ => None,
            })
            .collect();

        self.tree = self.do_insert(Some(self.tree.clone()), &comps, data.as_ref())?;

        Ok(())
    }

    pub fn remove<P>(&mut self, key: P) -> Result<()>
    where
        P: AsRef<path::Path>,
    {
        let repo = &self.index.repo;
        let key: &path::Path = key.as_ref();

        let comps: Vec<String> = key
            .components()
            .filter_map(|c| match c {
                path::Component::Normal(s) => Some(s.to_str()?.to_string()),
                _ => None,
            })
            .collect();

        let elapsed_from_epoch = {
            let dur = err_at!(Fatal, time::UNIX_EPOCH.elapsed())?;
            git2::Time::new(dur.as_secs() as i64, 0)
        };
        let update_ref = Some("HEAD");
        let author = err_at!(
            FailGitapi,
            git2::Signature::new("rdms/git", "", &elapsed_from_epoch)
        )?;
        let committer = err_at!(
            FailGitapi,
            git2::Signature::new("rdms/git", "", &elapsed_from_epoch)
        )?;
        let message = format!("insert {:?}", key);
        let parent = {
            let refn = err_at!(FailGitapi, repo.find_reference("HEAD"))?;
            err_at!(FailGitapi, refn.peel_to_commit())?
        };
        let tree = {
            let tree = self.index.get_db_root()?.into_tree().unwrap();
            self.do_remove(tree, &comps)?
        };

        err_at!(
            FailGitapi,
            repo.commit(
                update_ref,
                &author,
                &committer,
                &message,
                &tree,
                vec![&parent].as_slice(),
            )
        )?;

        Ok(())
    }

    fn do_insert(
        &self,
        tree: Option<git2::Tree<'a>>,
        comps: &[String],
        data: &[u8],
    ) -> Result<git2::Tree<'a>> {
        let comp = comps.first();
        let comps = &comps[1..];

        match (comp, tree) {
            (Some(comp), Some(tree)) if comps.is_empty() => match tree.get_name(comp) {
                Some(_) => self.insert_blob(Some(&tree), comp, data),
                None => self.insert_blob(Some(&tree), comp, data),
            },
            (Some(comp), Some(tree)) => match tree.get_name(comp) {
                Some(_) => self.insert_tree(
                    Some(&tree),
                    comp,
                    self.do_insert(None, comps, data)?.id(),
                ),
                None => self.do_insert(None, comps, data),
            },
            (Some(comp), None) if comps.is_empty() => self.insert_blob(None, comp, data),
            (Some(comp), None) => {
                self.insert_tree(None, comp, self.do_insert(None, comps, data)?.id())
            }
            (None, _) => unreachable!(),
        }
    }

    fn insert_blob(
        &self,
        tree: Option<&git2::Tree>,
        comp: &str,
        data: &[u8],
    ) -> Result<git2::Tree<'a>> {
        let odb = err_at!(FailGitapi, self.index.repo.odb())?;
        let mut builder = err_at!(FailGitapi, self.index.repo.treebuilder(tree))?;

        let oid = err_at!(FailGitapi, odb.write(git2::ObjectType::Blob, data))?;
        err_at!(FailGitapi, builder.insert(comp, oid, 0o100644))?;
        let oid = err_at!(FailGitapi, builder.write())?;

        let object = err_at!(FailGitapi, self.index.repo.find_object(oid, None))?;
        Ok(object.as_tree().unwrap().clone())
    }

    fn insert_tree(
        &self,
        tree: Option<&git2::Tree>,
        comp: &str,
        oid: git2::Oid,
    ) -> Result<git2::Tree<'a>> {
        let mut builder = err_at!(FailGitapi, self.index.repo.treebuilder(tree))?;

        err_at!(FailGitapi, builder.insert(comp, oid, 0o100644))?;
        let oid = err_at!(FailGitapi, builder.write())?;

        let object = err_at!(FailGitapi, self.index.repo.find_object(oid, None))?;
        Ok(object.as_tree().unwrap().clone())
    }

    fn do_remove(
        &self,
        tree: git2::Tree<'a>,
        comps: &[String],
    ) -> Result<git2::Tree<'a>> {
        let comp = comps.first();
        let comps = &comps[1..];

        match comp {
            Some(comp) if comps.is_empty() => self.remove_entry(&tree, comp),
            Some(comp) => match tree.get_name(comp) {
                Some(te) => match te.kind() {
                    Some(git2::ObjectType::Tree) => {
                        let obj = err_at!(FailGitapi, te.to_object(&self.index.repo))?;
                        self.do_remove(obj.into_tree().unwrap(), comps)
                    }
                    Some(_) | None => err_at!(KeyNotFound, msg: "missing key"),
                },
                None => err_at!(KeyNotFound, msg: "missing key"),
            },
            None => unreachable!(),
        }
    }

    fn remove_entry(&self, tree: &git2::Tree, comp: &str) -> Result<git2::Tree<'a>> {
        let mut builder = err_at!(FailGitapi, self.index.repo.treebuilder(Some(tree)))?;

        err_at!(FailGitapi, builder.remove(comp))?;
        let oid = err_at!(FailGitapi, builder.write())?;

        let object = err_at!(FailGitapi, self.index.repo.find_object(oid, None))?;
        Ok(object.as_tree().unwrap().clone())
    }
}

pub struct Entry {
    key: Box<path::Path>,
    data: Vec<u8>,
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Entry<{:?}>", self.key)
    }
}

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Entry<{:?}>", self.key)
    }
}

impl Entry {
    fn from_tree_entry(
        repo: &git2::Repository,
        mut rloc: path::PathBuf,
        tree_entry: git2::TreeEntry<'static>,
    ) -> Result<Self> {
        let key: Box<path::Path> = {
            rloc.push(tree_entry.name().unwrap());
            rloc.into()
        };
        let data = {
            let obj = err_at!(FailGitapi, tree_entry.to_object(repo))?;
            obj.as_blob().unwrap().content().to_vec()
        };

        let entry = Entry { key, data };
        Ok(entry)
    }
}

impl Entry {
    pub fn as_key(&self) -> &path::Path {
        self.key.as_ref()
    }

    pub fn as_key_str(&self) -> &str {
        self.key.to_str().unwrap()
    }

    pub fn as_blob(&self) -> &[u8] {
        &self.data
    }
}
