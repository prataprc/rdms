use std::{ffi, fs, path};

use rdms::{err_at, git::repo, trie, util::files, Error, Result};

use crate::Handler;

#[derive(Clone)]
pub struct WalkState<H>
where
    H: Handler,
{
    scan_dir: path::PathBuf,
    h: H,
    repos: Vec<repo::Repo>,
}

impl<H> WalkState<H>
where
    H: Handler + Clone,
{
    pub fn new(handler: H) -> Self {
        WalkState {
            scan_dir: path::PathBuf::default(),
            h: handler,
            repos: Vec::default(),
        }
    }

    pub fn to_scan_dir(&self) -> path::PathBuf {
        self.scan_dir.clone()
    }

    pub fn scan(mut self) -> Result<Self> {
        let mut iter = self.h.to_scan_dirs().into_iter();
        loop {
            match iter.next() {
                Some(scan_dir) => {
                    self.scan_dir = scan_dir.clone();
                    {
                        if let Ok(repo) = repo::Repo::from_loc(&scan_dir) {
                            self.repos.push(repo);
                        }
                    }
                    match files::walk(&scan_dir, self.clone(), check_dir_entry) {
                        Ok(ws) => break Ok(ws),
                        Err(err) => println!("skip scan_dir {:?}, {}", scan_dir, err),
                    };
                }
                None => break err_at!(InvalidFile, msg: "invalid scan dirs"),
            }
        }
    }

    pub fn into_repositories(self) -> Result<Vec<repo::Repo>> {
        let index = self.clone().into_trie();

        let mut repos: Vec<repo::Repo> = index
            .walk(Vec::<repo::Repo>::default(), |repos, _, _, value, _, _| {
                value.map(|repo| repos.push(repo.clone()));
                Ok(trie::WalkRes::Ok)
            })?
            .into_iter()
            .filter(|r| !files::is_excluded(&r.to_loc(), &self.h.to_exclude_dirs()))
            .collect();

        repos.sort_unstable_by_key(|r| r.to_last_commit_date(None).unwrap());

        Ok(repos)
    }

    pub fn into_trie(self) -> trie::Trie<ffi::OsString, repo::Repo> {
        self.repos.clone().into_iter().fold(trie::Trie::new(), |mut index, repo| {
            let comps: Vec<ffi::OsString> = path::PathBuf::from(&repo.to_loc())
                .components()
                .map(|c| c.as_os_str().to_os_string())
                .collect();
            index.set(&comps, repo);
            index
        })
    }
}

fn check_dir_entry<H>(
    walk_state: &mut WalkState<H>,
    parent: &path::Path,
    entry: &fs::DirEntry,
    _depth: usize,
    _breath: usize,
) -> Result<files::WalkRes>
where
    H: Handler,
{
    if let Some(".git") = entry.file_name().to_str() {
        Ok(files::WalkRes::SkipDir)
    } else {
        if let Ok(repo) = repo::Repo::from_entry(parent, entry) {
            walk_state.repos.push(repo);
        }
        Ok(files::WalkRes::Ok)
    }
}
