use git2::{Repository, RepositoryInitMode, RepositoryInitOptions, RepositoryOpenFlags};

use std::{ffi, file, fmt, fs, ops::Bound, ops::RangeBounds, path, result};

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

    pub fn close(self) -> Result<()> {
        Ok(())
    }

    pub fn purge(self) -> Result<()> {
        err_at!(IOError, fs::remove_dir(&self.config.loc_repo))
    }
}

impl Index {
    pub fn as_config(&self) -> &Config {
        &self.config
    }

    pub fn len(&self) -> Result<usize> {
        let count: usize = self.iter()?.map(|_| 1).sum();
        Ok(count)
    }

    pub fn is_empty(&self) -> bool {
        match self.len() {
            Ok(n) if n == 0 => true,
            Ok(_) => false,
            Err(_) => false,
        }
    }
}

impl Index {
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

    pub fn get_versions<P>(&self, key: P) -> Result<Option<Entry>>
    where
        P: Clone + AsRef<path::Path>,
    {
        self.get(key)
    }

    pub fn iter(&self) -> Result<IterLevel> {
        let val = {
            let tree = self.get_db_root()?.into_tree().unwrap();
            IterLevel::forward(&self.repo, "".into(), &tree)?
        };
        Ok(val)
    }

    pub fn iter_versions(&self) -> Result<IterLevel> {
        self.iter()
    }

    pub fn range<R, P>(&self, range: R) -> Result<Range<P>>
    where
        R: RangeBounds<P>,
        P: Clone + AsRef<path::Path>,
    {
        let iter = {
            let tree = self.get_db_root()?.into_tree().unwrap();
            IterLevel::forward(&self.repo, "".into(), &tree)?
        };
        let val = {
            let low = Some(range.start_bound().cloned());
            let high = range.end_bound().cloned();
            Range { iter, low, high }
        };

        Ok(val)
    }

    pub fn range_versions<R, P>(&self, range: R) -> Result<Range<P>>
    where
        R: RangeBounds<P>,
        P: Clone + AsRef<path::Path>,
    {
        self.range(range)
    }

    pub fn reverse<R, P>(&self, range: R) -> Result<Reverse<P>>
    where
        R: RangeBounds<P>,
        P: Clone + AsRef<path::Path>,
    {
        let iter = {
            let tree = self.get_db_root()?.into_tree().unwrap();
            IterLevel::reverse(&self.repo, "".into(), &tree)?
        };
        let val = {
            let low = range.start_bound().cloned();
            let high = Some(range.end_bound().cloned());
            Reverse { iter, low, high }
        };

        Ok(val)
    }

    pub fn reverse_versions<R, P>(&self, range: R) -> Result<Reverse<P>>
    where
        R: RangeBounds<P>,
        P: Clone + AsRef<path::Path>,
    {
        self.reverse(range)
    }
}

impl Index {
    pub fn insert(&mut self, _key: &ffi::OsStr, _value: String) -> Option<String> {
        todo!()
    }

    pub fn remove(&mut self, _key: &ffi::OsStr) -> Option<String> {
        todo!()
    }

    pub fn commit<I>(&mut self, _iter: I) -> Option<usize>
    where
        I: Iterator<Item = Entry>,
    {
        todo!()
    }
}

impl Index {
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

    // convert a key to its components, there are few criterias in supplying the key:
    // a. must be a valid string
    // b. must not start with root or drive prefix
    // c. must not start with current directory or parent directory.
    #[allow(dead_code)] // TODO
    fn key_to_components(key: &path::Path) -> Vec<String> {
        key.components()
            .filter_map(|c| match c {
                path::Component::Normal(s) => Some(s.to_str()?.to_string()),
                _ => None,
            })
            .collect()
    }

    // Entres in tree in reverse sort order.
    fn tree_entries(tree: &git2::Tree, rev: bool) -> Vec<IterEntry<'static>> {
        let mut items: Vec<IterEntry> = tree
            .iter()
            .filter(|e| e.name().is_some())
            .map(|e| IterEntry::from(e.to_owned()))
            .collect();
        match rev {
            false => items
                .sort_by(|a, b| a.to_name().unwrap().cmp(b.to_name().unwrap()).reverse()),
            true => items.sort_by(|a, b| a.to_name().unwrap().cmp(b.to_name().unwrap())),
        }
        items
    }
}

// iterate from low to high
pub struct Range<'a, P>
where
    P: AsRef<path::Path>,
{
    iter: IterLevel<'a>,
    low: Option<Bound<P>>,
    high: Bound<P>,
}

impl<'a, P> Iterator for Range<'a, P>
where
    P: AsRef<path::Path>,
{
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.low.take() {
            Some(low) => loop {
                let e = match self.iter.next()? {
                    Ok(e) => e,
                    Err(e) => return Some(Err(e)),
                };
                let ekey: &path::Path = e.key.as_ref();

                match &low {
                    Bound::Unbounded => break Some(Ok(e)),
                    Bound::Included(p) if ekey.ge(p.as_ref()) => break Some(Ok(e)),
                    Bound::Included(_) => (),
                    Bound::Excluded(p) if ekey.gt(p.as_ref()) => break Some(Ok(e)),
                    Bound::Excluded(_) => (),
                }
            },
            None => {
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
    }
}

// iterate from high to low
pub struct Reverse<'a, P>
where
    P: AsRef<path::Path>,
{
    iter: IterLevel<'a>,
    low: Bound<P>,
    high: Option<Bound<P>>,
}

impl<'a, P> Iterator for Reverse<'a, P>
where
    P: AsRef<path::Path>,
{
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.high.take() {
            Some(high) => loop {
                let e = match self.iter.next()? {
                    Ok(e) => e,
                    Err(e) => return Some(Err(e)),
                };
                let ekey: &path::Path = e.key.as_ref();

                match &high {
                    Bound::Unbounded => break Some(Ok(e)),
                    Bound::Included(p) if ekey.le(p.as_ref()) => break Some(Ok(e)),
                    Bound::Included(_) => (),
                    Bound::Excluded(p) if ekey.lt(p.as_ref()) => break Some(Ok(e)),
                    Bound::Excluded(_) => (),
                }
            },
            None => {
                let e = match self.iter.next()? {
                    Ok(e) => e,
                    Err(e) => return Some(Err(e)),
                };
                let ekey: &path::Path = e.key.as_ref();

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
    fn to_name(&self) -> Option<&str> {
        match self {
            IterEntry::Entry { te } => te.name(),
            IterEntry::Dir { .. } => None,
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
    fn forward(
        repo: &'a git2::Repository,
        rloc: path::PathBuf, // following `tree` argument is under `rloc` path.
        tree: &git2::Tree,
    ) -> Result<IterLevel<'a>> {
        let val = IterLevel {
            repo,
            rloc,
            items: Index::tree_entries(tree, false /*rev*/),
            rev: false,
        };

        Ok(val)
    }

    fn reverse(
        repo: &'a git2::Repository,
        rloc: path::PathBuf, // following `tree` argument is under `rloc` path.
        tree: &git2::Tree,
    ) -> Result<IterLevel<'a>> {
        let val = IterLevel {
            repo,
            rloc,
            items: Index::tree_entries(tree, true /*rev*/),
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
        loop {
            let item = self.items.pop()?;
            match item {
                IterEntry::Entry { te } => match te.kind().unwrap() {
                    git2::ObjectType::Blob => {
                        // println!("{} iterl-blob {}", n, te.name().unwrap());
                        let entry = iter_result!(Entry::from_tree_entry(
                            &self.repo,
                            self.rloc.clone(),
                            te
                        ));
                        break Some(Ok(entry));
                    }
                    git2::ObjectType::Tree => {
                        // println!("{} iterl-tree {}", n, te.name().unwrap());
                        let tree = {
                            let val = iter_result!(te.to_object(&self.repo));
                            val.into_tree().unwrap()
                        };
                        let rloc: path::PathBuf =
                            [self.rloc.clone(), te.name().unwrap().into()]
                                .iter()
                                .collect();

                        let iter = match self.rev {
                            false => {
                                iter_result!(IterLevel::forward(&self.repo, rloc, &tree))
                            }
                            true => {
                                iter_result!(IterLevel::reverse(&self.repo, rloc, &tree))
                            }
                        };
                        self.items.push(iter.into());
                        break self.next();
                    }
                    _ => unreachable!(),
                },
                IterEntry::Dir { mut iter } => match iter.next() {
                    Some(entry) => {
                        // println!("{} iterl-dir {:?}", n, entry.as_ref().unwrap().key);
                        self.items.push(iter.into());
                        break Some(entry);
                    }
                    None => (),
                },
            }
        }
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
