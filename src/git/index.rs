use git2::{Repository, RepositoryInitMode, RepositoryInitOptions, RepositoryOpenFlags};

use std::{ffi, file, fs, ops::RangeBounds, path};

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
            let path = config.loc_repo.clone();
            err_at!(Fatal, Repository::init_opts(path, &options))?
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
        //let count: usize = self.iter()?.map(|_| 1).sum();
        //Ok(count)
        todo!()
    }

    pub fn is_empty(&self) -> bool {
        todo!()
    }

    pub fn footprint(&self) -> Result<isize> {
        todo!()
    }

    pub fn to_stats(&self) -> Result<()> {
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}

impl Index {
    pub fn get(&self, _key: &ffi::OsStr) -> Option<String> {
        todo!()
    }

    pub fn get_versions(&self, _key: &ffi::OsStr) -> Option<Entry> {
        todo!()
    }

    pub fn iter(&self) -> Result<Iter> {
        let tree = self.get_db_root()?.into_tree().unwrap();
        Iter::new(&self.repo, Vec::default(), &tree)
    }

    pub fn iter_versions(&self) -> Result<Iter> {
        self.iter()
    }

    pub fn range<R>(&self, _range: R) -> Result<Iter>
    where
        R: RangeBounds<ffi::OsStr>,
    {
        todo!()
    }

    pub fn range_versions<R>(&self, _range: R) -> Result<Iter>
    where
        R: RangeBounds<ffi::OsStr>,
    {
        todo!()
    }

    pub fn reverse<R>(&self, _range: R) -> Result<Iter>
    where
        R: RangeBounds<ffi::OsStr>,
    {
        todo!()
    }

    pub fn reverse_versions<R>(&self, _range: R) -> Result<Iter>
    where
        R: RangeBounds<ffi::OsStr>,
    {
        todo!()
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
}

enum IterEntry<'a> {
    Entry { te: git2::TreeEntry<'static> },
    Dir { iter: Iter<'a> },
}

impl<'a> From<git2::TreeEntry<'static>> for IterEntry<'a> {
    fn from(te: git2::TreeEntry<'static>) -> IterEntry {
        IterEntry::Entry { te }
    }
}

impl<'a> From<Iter<'a>> for IterEntry<'a> {
    fn from(iter: Iter<'a>) -> IterEntry<'a> {
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

pub struct Iter<'a> {
    repo: &'a git2::Repository,
    path: Vec<String>,
    items: Vec<IterEntry<'a>>,
}

impl<'a> Iter<'a> {
    fn new(
        repo: &'a git2::Repository,
        path: Vec<String>,
        tree: &git2::Tree,
    ) -> Result<Iter<'a>> {
        let val = Iter {
            repo,
            path,
            items: Self::tree_entries(tree),
        };

        Ok(val)
    }

    fn tree_entries(tree: &git2::Tree) -> Vec<IterEntry<'a>> {
        let mut items: Vec<IterEntry> = tree
            .iter()
            .filter(|e| e.name().is_some())
            .map(|e| IterEntry::from(e.to_owned()))
            .collect();
        items.sort_by(|a, b| a.to_name().unwrap().cmp(b.to_name().unwrap()).reverse());
        items
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.items.pop()?;
        match item {
            IterEntry::Entry { te } => match te.kind().unwrap() {
                git2::ObjectType::Blob => {
                    let entry = Entry::from_tree_entry(te, self.path.to_vec());
                    Some(Ok(entry))
                }
                git2::ObjectType::Tree => {
                    let tree = {
                        let val = iter_result!(te.to_object(&self.repo));
                        val.into_tree().unwrap()
                    };
                    let path = {
                        let mut path = self.path.to_vec();
                        path.push(te.name().unwrap().to_string());
                        path
                    };
                    let iter = iter_result!(Iter::new(&self.repo, path, &tree));
                    self.items.push(iter.into());
                    self.next()
                }
                _ => unreachable!(),
            },
            IterEntry::Dir { mut iter } => match iter.next() {
                Some(entry) => {
                    self.items.push(iter.into());
                    Some(entry)
                }
                None => self.next(),
            },
        }
    }
}

pub struct Entry {
    pub key: String,
    pub tree_entry: git2::TreeEntry<'static>,
}

impl Entry {
    fn from_tree_entry(
        tree_entry: git2::TreeEntry<'static>,
        mut path: Vec<String>,
    ) -> Self {
        path.push(tree_entry.name().unwrap().to_string());

        let key = path
            .iter()
            .collect::<path::PathBuf>()
            .to_str()
            .unwrap()
            .to_string();

        Entry { key, tree_entry }
    }
}
