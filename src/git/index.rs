use git2::{Repository, RepositoryInitMode, RepositoryInitOptions, RepositoryOpenFlags};

use std::file;

use crate::{
    git::{Config, Permissions},
    Error, Result,
};

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
}

impl Index {
    //pub fn path(&self) -> path::Path {
    //    todo!()
    //}

    //pub fn is_empty(&self) -> bool {
    //    todo!()
    //}

    //pub fn len(&self) -> usize {
    //    todo!()
    //}
}

impl Index {
    //pub fn get<Q>(&self, key: &Q) -> Option<V>
    //where
    //    K: Borrow<Q>,
    //    Q: PartialEq,
    //{
    //    todo!()
    //}

    //pub fn set(&mut self, key: K, value: V) -> Option<V>
    //where
    //    K: PartialEq,
    //{
    //    todo!()
    //}

    //pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    //where
    //    K: Borrow<Q>,
    //    Q: PartialEq,
    //{
    //    todo!()
    //}

    //pub fn iter(&self) -> Iter<'_, K, V> {
    //    todo!()
    //}

    //pub fn iter_keys(&self) -> IterKeys<'_, K> {
    //    todo!()
    //}

    //pub fn range<Q: ?Sized, R>(&self, range: R) -> Range<'_, K, V, R, Q>
    //where
    //    K: Borrow<Q>,
    //    R: RangeBounds<Q>,
    //    Q: Ord,
    //{
    //    todo!()
    //}

    //pub fn reverse<Q: ?Sized, R>(&self, range: R) -> Reverse<'_, K, V, R, Q>
    //where
    //    K: Borrow<Q>,
    //    R: RangeBounds<Q>,
    //    Q: Ord,
    //{
    //    todo!()
    //}
}
