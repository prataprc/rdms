use colored::Colorize;
use prettytable::{cell, row};

use std::{fs, path};

use rdms::{err_at, util::print::PrettyRow, Error, Result};

#[derive(Copy, Clone, Debug)]
pub enum Age {
    Hot,
    Cold,
    Frozen,
}

#[allow(dead_code)]
pub struct Repo {
    loc: path::PathBuf,
    repo: git2::Repository,

    hot: Option<usize>,
    cold: Option<usize>,
    ignored: bool,

    branches: Vec<Branch>,
    branch: Option<Branch>,
    last_commit_date: i64, // seconds since epoch
}

impl Clone for Repo {
    fn clone(&self) -> Self {
        Repo {
            loc: self.loc.clone(),
            repo: Repo::open(self.loc.clone()).unwrap().unwrap(),

            hot: self.hot,
            cold: self.cold,
            ignored: self.ignored,
            branches: self.branches.clone(),
            branch: self.branch.clone(),
            last_commit_date: self.last_commit_date, // seconds since epoch
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
                let (branches, branch) = get_repo_branches(&repo)?;
                let last_commit_date = branches
                    .iter()
                    .map(|br| br.last_commit_date)
                    .max()
                    .unwrap_or(0);

                let repo = Repo {
                    loc,
                    repo,

                    hot: None,
                    cold: None,
                    ignored: false,

                    branches,
                    branch,
                    last_commit_date,
                };
                Ok(repo)
            }
            None => err_at!(InvalidInput, msg: "{:?} not a repo", loc),
        }
    }

    pub fn set_ignored(&mut self, ignored: bool) -> &mut Self {
        self.ignored = ignored;
        self
    }

    pub fn set_hot(&mut self, hot: Option<usize>) -> &mut Self {
        self.hot = hot;
        self
    }

    pub fn set_cold(&mut self, cold: Option<usize>) -> &mut Self {
        self.cold = cold;
        self
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
    #[allow(dead_code)]
    fn is_symlink(&self) -> Result<bool> {
        Ok(err_at!(IOError, fs::metadata(&self.loc))?.is_symlink())
    }

    pub fn to_loc(&self) -> path::PathBuf {
        self.loc.clone()
    }

    pub fn to_last_commit_date(&self) -> i64 {
        self.last_commit_date
    }

    pub fn to_deltas(&self) -> Result<Vec<git2::Delta>> {
        if self.repo.is_bare() {
            Ok(vec![])
        } else {
            let mut dopts = make_diff_options(self.ignored);
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

    pub fn to_stash_count(&mut self) -> Result<usize> {
        let mut n = 0;
        let res = self.repo.stash_foreach(|_, _, _| {
            n += 1;
            true
        });
        err_at!(Fatal, res)?;
        Ok(n)
    }

    pub fn to_age(&self, hot: Option<usize>, cold: Option<usize>) -> Result<Age> {
        let hot = hot.map(|hot| 3600 * 24 * 30 * hot as i64); // in seconds;
        let cold = cold.map(|cold| 3600 * 24 * 30 * cold as i64); // in seconds;

        let now = chrono::offset::Local::now().naive_local();
        let dt = chrono::NaiveDateTime::from_timestamp(self.last_commit_date, 0);

        let age = match hot {
            Some(secs) if dt < (now - chrono::Duration::seconds(secs)) => match cold {
                Some(secs) if dt < (now - chrono::Duration::seconds(secs)) => Age::Frozen,
                Some(_) | None => Age::Cold,
            },
            Some(_) => Age::Hot,
            None => match cold {
                Some(secs) if dt < (now - chrono::Duration::seconds(secs)) => Age::Cold,
                Some(_) | None => Age::Hot,
            },
        };

        Ok(age)
    }
}

impl PrettyRow for Repo {
    fn to_format() -> prettytable::format::TableFormat {
        *prettytable::format::consts::FORMAT_CLEAN
    }

    fn to_head() -> prettytable::Row {
        row![Fy => "Dir", "Commit", "State", "Branches"]
    }

    fn to_row(&mut self) -> prettytable::Row {
        let (display_state, attention) = display_repository_state(self);
        let brs = display_branches(self);
        let commit_date = chrono::NaiveDateTime::from_timestamp(self.last_commit_date, 0)
            .format("%Y-%b-%d")
            .to_string();
        let dir = {
            let comps: Vec<path::Component> = self.loc.components().collect();
            let p: path::PathBuf = comps.into_iter().rev().take(2).rev().collect();
            p.as_os_str()
                .to_str()
                .map(|s| s.to_string())
                .unwrap_or("--".to_string())
        };

        let age = self.to_age(self.hot, self.cold).unwrap();
        let color = match (attention, age) {
            (true, _) => colored::Color::Red,
            (false, Age::Hot) => colored::Color::TrueColor {
                r: 255,
                g: 255,
                b: 255,
            },
            (false, Age::Cold) => colored::Color::TrueColor {
                r: 180,
                g: 180,
                b: 180,
            },
            (false, Age::Frozen) => colored::Color::TrueColor {
                r: 100,
                g: 100,
                b: 100,
            },
        };

        row![
            dir.color(color),
            commit_date.color(color),
            display_state,
            brs,
        ]
    }
}

fn display_repository_state(repo: &mut Repo) -> (String, bool) {
    // TODO: figure out whether there are any staged changes.

    let mut states = vec![];
    let mut attention = false;

    {
        if repo.repo.is_bare() {
            states.push("⛼".to_string().yellow());
        }
    }

    {
        let s: colored::ColoredString = match &repo.repo.state() {
            git2::RepositoryState::Clean => "".into(),
            git2::RepositoryState::Merge => "merge".red(),
            git2::RepositoryState::Revert => "revert".red(),
            git2::RepositoryState::RevertSequence => "revert-seq".red(),
            git2::RepositoryState::CherryPick => "cherry-pick".red(),
            git2::RepositoryState::CherryPickSequence => "cherry-pick-seq".red(),
            git2::RepositoryState::Bisect => "bisect".red(),
            git2::RepositoryState::Rebase => "rebase".red(),
            git2::RepositoryState::RebaseInteractive => "rebase-interactive".red(),
            git2::RepositoryState::RebaseMerge => "rebase-merge".red(),
            git2::RepositoryState::ApplyMailbox => "apply-mailbox".red(),
            git2::RepositoryState::ApplyMailboxOrRebase => "apply-mailbox/rebase".red(),
        };
        attention = attention || !s.is_empty();
        states.push(s);
    }

    {
        let mods = display_modifed(repo);
        attention = attention || mods.iter().any(|s| !s.is_plain());
        states.extend_from_slice(&mods)
    }

    {
        let tags = repo.to_tags().unwrap();
        if !tags.is_empty() {
            states.push("🏷".magenta());
        }
    }

    {
        if repo.to_stash_count().unwrap() > 0 {
            states.push("🛍".blue());
        }
    }

    states = states.into_iter().filter(|s| !s.is_empty()).collect();
    match states.is_empty() {
        true => ("👍".green().to_string(), attention),
        false => (
            states
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
                .join(" "),
            attention,
        ),
    }
}

fn display_modifed(repo: &Repo) -> Vec<colored::ColoredString> {
    let mut mods = vec![];
    let deltas = repo.to_deltas().unwrap();

    if deltas.iter().any(|d| {
        matches!(
            d,
            git2::Delta::Added | git2::Delta::Deleted | git2::Delta::Modified
        )
    }) {
        mods.push("✎".red())
    }
    if deltas.iter().any(|d| {
        matches!(
            d,
            git2::Delta::Renamed
                | git2::Delta::Copied
                | git2::Delta::Ignored
                | git2::Delta::Untracked
        )
    }) {
        mods.push("☕".red())
    }
    if deltas.iter().any(|d| {
        matches!(
            d,
            git2::Delta::Typechange | git2::Delta::Unreadable | git2::Delta::Conflicted
        )
    }) {
        mods.push("☒".red())
    }

    mods
}

fn display_branches(repo: &Repo) -> String {
    repo.branches
        .iter()
        .filter_map(|br| match (&repo.branch, &br.upstream) {
            (Some(hbr), Some(ups)) if hbr.name == br.name && br.upstream_synced => {
                Some(format!("{} <-> {}", br.name, ups.name).yellow())
            }
            (Some(hbr), Some(ups)) if hbr.name == br.name && br.upstream_synced => {
                Some(format!("{} <-> {}", br.name, ups.name).cyan())
            }
            (Some(_), Some(_)) if br.upstream_synced => None,
            (Some(_), Some(ups)) => Some(format!("{} <->{}", br.name, ups.name).cyan()),
            _ => Some(br.name.as_str().into()),
        })
        .map(|s| s.to_string())
        .collect::<Vec<String>>()
        .join("\n")
}

#[derive(Clone)]
pub struct Branch {
    name: String,
    branch_type: git2::BranchType, // git2::BranchType,

    kind: String, // git2::ReferenceType,
    shorthand: String,
    resolved_kind: String, // git2::ReferenceType
    resolved_shorthand: String,
    target: String, // git2::Oid,

    last_commit_date: i64, // seconds since epoch.
    is_head: bool,
    is_remote: bool,
    upstream: Option<Box<Branch>>,
    upstream_synced: bool,
}

impl Branch {
    #[allow(dead_code)]
    pub fn display_branch_type(&self) -> String {
        match self.branch_type {
            git2::BranchType::Local => "local".to_string(),
            git2::BranchType::Remote => "remote".to_string(),
        }
    }
}
fn get_repo_branches(repo: &git2::Repository) -> Result<(Vec<Branch>, Option<Branch>)> {
    let path = repo.path();

    let (mut branches, mut branch) = (vec![], None);
    for res in err_at!(Fatal, repo.branches(None), "Repository::branches")?.into_iter() {
        let (br, branch_type) = err_at!(Fatal, res, "branch iter for {:?}", path)?;
        let br = get_repo_branch(path, br, branch_type)?;

        if !br.is_remote {
            if br.is_head {
                branch = Some(br.clone())
            }
            branches.push(br)
        }
    }

    Ok((branches, branch))
}

fn get_repo_branch(
    path: &path::Path,
    branch: git2::Branch,
    branch_type: git2::BranchType,
) -> Result<Branch> {
    let name = err_at!(Fatal, branch.name(), "fail to get {:?}", path)?
        .unwrap_or("")
        .to_string();
    let is_head = branch.is_head();
    let is_remote = branch.get().is_remote();
    let (upstream, upstream_synced) = match branch.upstream().ok() {
        Some(upstream) => {
            let upstream_synced = upstream.get() == branch.get();
            (
                Some(get_repo_branch(path, upstream, branch_type)?),
                upstream_synced,
            )
        }
        None => (None, false),
    };
    let last_commit_date = branch.get().peel_to_commit().unwrap().time().seconds();

    let refrn = branch.into_reference();

    let kind = get_reference_kind(&refrn);
    let shorthand = get_reference_shorthand(&refrn);
    let resolved_kind = refrn
        .resolve()
        .ok()
        .map(|r| get_reference_kind(&r))
        .unwrap_or_else(|| "".to_string());
    let resolved_shorthand = refrn
        .resolve()
        .ok()
        .map(|r| get_reference_shorthand(&r))
        .unwrap_or_else(|| "".to_string());
    let target = refrn
        .target()
        .map(|o| o.to_string())
        .unwrap_or_else(|| "".to_string());

    let branch = Branch {
        name,
        branch_type,

        kind,
        shorthand,
        resolved_kind,
        resolved_shorthand,
        target,

        last_commit_date,
        is_head,
        is_remote,
        upstream: upstream.map(Box::new),
        upstream_synced,
    };

    Ok(branch)
}

pub fn make_diff_options(ignored: bool) -> git2::DiffOptions {
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

pub fn get_reference_kind(refrn: &git2::Reference) -> String {
    refrn
        .kind()
        .map(|k| k.to_string())
        .unwrap_or_else(|| "".to_string())
}

pub fn get_reference_shorthand(refrn: &git2::Reference) -> String {
    refrn
        .shorthand()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "".to_string())
}
