use std::{fs, path};

use crate::{Error, Result};

/// Repo loads a single git repository along with its branches, it is an abstraction
/// over libgit2 API.
pub struct Repo {
    loc: path::PathBuf,
    repo: git2::Repository,
}

impl Clone for Repo {
    fn clone(&self) -> Self {
        let repo = Repo::open(self.loc.clone()).unwrap().unwrap();
        Repo {
            loc: self.loc.clone(),
            repo,
        }
    }
}

impl Repo {
    pub fn from_entry(parent: &path::Path, entry: &fs::DirEntry) -> Result<Repo> {
        let loc: path::PathBuf = vec![parent.to_path_buf(), entry.file_name().into()]
            .into_iter()
            .collect();

        match Repo::open(loc.clone())? {
            Some(repo) => {
                let repo = Repo { loc, repo };
                Ok(repo)
            }
            None => err_at!(InvalidInput, msg: "{:?} not a repo", loc),
        }
    }

    fn work_flags() -> git2::RepositoryOpenFlags {
        git2::RepositoryOpenFlags::NO_SEARCH | git2::RepositoryOpenFlags::CROSS_FS
    }

    fn bare_flags() -> git2::RepositoryOpenFlags {
        git2::RepositoryOpenFlags::NO_SEARCH
            | git2::RepositoryOpenFlags::CROSS_FS
            | git2::RepositoryOpenFlags::NO_DOTGIT
            | git2::RepositoryOpenFlags::BARE
    }

    fn open(loc: path::PathBuf) -> Result<Option<git2::Repository>> {
        let ceildrs = Vec::<String>::default().into_iter();
        let repo1 = git2::Repository::open_ext(&loc, Self::work_flags(), ceildrs.clone());
        let repo2 = git2::Repository::open_ext(&loc, Self::bare_flags(), ceildrs);

        let res = match repo1 {
            Ok(repo) => Some(repo),
            Err(_) => match repo2 {
                Ok(repo) => Some(repo),
                Err(err) if err.code() == git2::ErrorCode::NotFound => None,
                Err(err) => err_at!(Fatal, Err(err))?,
            },
        };

        Ok(res)
    }
}

impl Repo {
    pub fn is_bare(&self) -> bool {
        self.repo.is_bare()
    }

    pub fn to_loc(&self) -> path::PathBuf {
        self.loc.clone()
    }

    pub fn to_repo_state(&self) -> git2::RepositoryState {
        self.repo.state()
    }

    pub fn to_last_commit_date(
        &self,
        branch: Option<String>,
    ) -> Result<Option<chrono::NaiveDateTime>> {
        let dt = match branch {
            Some(name) => match self.to_branch(name)? {
                Some(br) => {
                    let secs = br.get().peel_to_commit().unwrap().time().seconds();
                    Some(chrono::NaiveDateTime::from_timestamp(secs, 0))
                }
                _ => None,
            },
            None => {
                let typ = Some(git2::BranchType::Local);
                let mut dts = vec![];
                for item in err_at!(Fatal, self.repo.branches(typ))? {
                    let (br, _) = err_at!(Fatal, item)?;
                    let secs = br.get().peel_to_commit().unwrap().time().seconds();
                    dts.push(chrono::NaiveDateTime::from_timestamp(secs, 0))
                }
                dts.into_iter().max()
            }
        };

        Ok(dt)
    }

    pub fn to_branch(&self, name: String) -> Result<Option<git2::Branch>> {
        for item in err_at!(Fatal, self.repo.branches(None))? {
            let (br, _) = err_at!(Fatal, item)?;
            if Self::branch_name(&br) == name {
                return Ok(Some(br));
            }
        }
        Ok(None)
    }

    pub fn to_branches(&self, t: Option<git2::BranchType>) -> Result<Vec<git2::Branch>> {
        let mut branches = vec![];
        for item in err_at!(Fatal, self.repo.branches(t))? {
            let (br, _) = err_at!(Fatal, item)?;
            branches.push(br);
        }

        Ok(branches)
    }

    pub fn to_current_branch(&self) -> Result<Option<git2::Branch>> {
        for br in self.to_branches(Some(git2::BranchType::Local))?.into_iter() {
            if br.is_head() {
                return Ok(Some(br));
            }
        }
        Ok(None)
    }

    pub fn to_deltas(&self, ignored: bool) -> Result<Vec<git2::Delta>> {
        if self.repo.is_bare() {
            Ok(vec![])
        } else {
            let mut dopts = make_diff_options(ignored);
            let diff = err_at!(
                Fatal,
                self.repo.diff_index_to_workdir(None, Some(&mut dopts))
            )?;
            Ok(diff.deltas().map(|d| d.status()).collect())
        }
    }

    pub fn to_tags(&self) -> Result<Vec<String>> {
        Ok(err_at!(
            Fatal,
            self.repo.tag_names(None),
            "Branch::tag_names {:?}",
            self.loc
        )?
        .iter()
        .filter_map(|o| o.map(|s| s.to_string()))
        .collect())
    }

    pub fn to_references(&self) -> Result<Vec<git2::Reference>> {
        let mut refns = vec![];
        for item in err_at!(Fatal, self.repo.references())? {
            refns.push(err_at!(Fatal, item)?);
        }
        Ok(refns)
    }

    pub fn to_statuses(
        &self,
        mut sopts: Option<git2::StatusOptions>,
    ) -> Result<Vec<git2::Status>> {
        Ok(err_at!(Fatal, self.repo.statuses(sopts.as_mut()))?
            .iter()
            .map(|s| s.status())
            .collect())
    }

    pub fn branch_upstream<'a>(
        &'a self,
        br: git2::Branch<'a>,
    ) -> Option<(git2::Branch<'a>, bool)> {
        let upstream = br.upstream().ok()?;
        let upstream_synced = upstream.get() == br.get();
        Some((upstream, upstream_synced))
    }
}

impl Repo {
    pub fn branch_name(br: &git2::Branch) -> String {
        use std::iter::FromIterator;

        match br.name().ok().flatten() {
            Some(name) => name.to_string(),
            None => {
                let s = format!("{}", br.get().peel_to_commit().unwrap().id());
                String::from_iter(s.chars().take(6))
            }
        }
    }
}

fn make_diff_options(ignored: bool) -> git2::DiffOptions {
    let mut dopts = git2::DiffOptions::new();
    dopts
        .include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(ignored)
        .recurse_ignored_dirs(ignored)
        .ignore_filemode(false)
        .ignore_submodules(false)
        .include_unreadable(true);
    dopts
}
