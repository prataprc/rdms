use git2::{Repository, RepositoryInitMode, RepositoryInitOptions, RepositoryOpenFlags};

use std::{ffi, file, ops::RangeBounds};

use crate::{
    db,
    git::{Config, Permissions},
    Error, Result,
};

pub type Entry = db::Entry<ffi::OsString, db::Binary>;

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
        todo!()
    }

    pub fn purge(self) -> Result<()> {
        todo!()
    }
}

impl Index {
    pub fn as_config(&self) -> &Config {
        &self.config
    }

    pub fn len(&self) -> Result<usize> {
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

    pub fn iter(&self, _key: &ffi::OsStr) -> Result<Iter> {
        todo!()
    }

    pub fn iter_versions<K, V>(&self, _key: &K) -> Result<Iter> {
        todo!()
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

pub struct Iter {
    //
}
