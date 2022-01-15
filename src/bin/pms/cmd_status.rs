use colored::Colorize;
use prettytable::{cell, row};
use serde::Deserialize;

use std::{convert::TryFrom, ffi, fs, path};

use crate::SubCommand;
use rdms::{
    err_at, trie,
    util::{
        files,
        print::{self, PrettyRow},
    },
    Error, Result,
};

#[derive(Clone)]
pub struct Opt {
    pub ignored: bool,
    pub force_color: bool,
    pub loc_toml: Option<path::PathBuf>,
    pub profile: Profile, // toml options
}

#[derive(Clone, Default)]
pub struct Profile {
    hot: Option<usize>,
    cold: Option<usize>,
    scan_dirs: Vec<path::PathBuf>,
    exclude_dirs: Vec<path::PathBuf>,
}

#[derive(Clone, Deserialize)]
pub struct TomlProfile {
    hot: Option<usize>,  // in months
    cold: Option<usize>, // in months
    scan_dirs: Option<Vec<path::PathBuf>>,
    exclude_dirs: Option<Vec<path::PathBuf>>,
}

impl From<TomlProfile> for Profile {
    fn from(p: TomlProfile) -> Profile {
        Profile {
            hot: p.hot,
            cold: p.cold,
            scan_dirs: p.scan_dirs.unwrap_or_else(|| vec![]),
            exclude_dirs: p.exclude_dirs.unwrap_or_else(|| vec![]),
        }
    }
}

impl TryFrom<crate::SubCommand> for Opt {
    type Error = Error;

    fn try_from(subcmd: crate::SubCommand) -> Result<Opt> {
        let opt = match subcmd {
            SubCommand::Status {
                loc,
                ignored,
                force_color,
                toml,
            } => {
                let loc_toml = files::find_config(toml, &["pms.toml", ".pms.toml"]);
                let mut profile = match loc_toml.as_ref() {
                    Some(loc_toml) => {
                        files::load_toml::<_, TomlProfile>(loc_toml)?.into()
                    }
                    None => Profile::default(),
                };

                loc.map(|loc| profile.scan_dirs.push(loc.into()));

                Opt {
                    ignored,
                    force_color,
                    loc_toml,
                    profile,
                }
            }
        };

        Ok(opt)
    }
}

#[derive(Copy, Clone, Debug)]
enum Age {
    Hot,
    Cold,
    Frozen,
}

#[allow(dead_code)]
pub struct Repo {
    loc: path::PathBuf,
    repo: git2::Repository,
    age: Age,
    is_bare: bool,
    state: git2::RepositoryState,
    deltas: Vec<git2::Delta>,
    is_staged: bool,
    is_sync_remote: bool,
    branches: Vec<Branch>,
    branch: Option<Branch>,
    tags: Vec<String>,
    stashs: usize,
    last_commit_date: i64, // seconds since epoch
}

impl Clone for Repo {
    fn clone(&self) -> Self {
        Repo {
            loc: self.loc.clone(),
            repo: Repo::open(self.loc.clone()).unwrap().unwrap(),
            age: self.age.clone(),
            is_bare: self.is_bare,
            state: self.state.clone(),
            deltas: self.deltas.clone(),
            is_staged: self.is_staged,
            is_sync_remote: self.is_sync_remote,
            branches: self.branches.clone(),
            branch: self.branch.clone(),
            tags: self.tags.clone(),
            stashs: self.stashs.clone(),
            last_commit_date: self.last_commit_date, // seconds since epoch
        }
    }
}

impl Repo {
    pub fn work_flags() -> git2::RepositoryOpenFlags {
        git2::RepositoryOpenFlags::NO_SEARCH | git2::RepositoryOpenFlags::CROSS_FS
    }

    pub fn bare_flags() -> git2::RepositoryOpenFlags {
        git2::RepositoryOpenFlags::NO_SEARCH
            | git2::RepositoryOpenFlags::CROSS_FS
            | git2::RepositoryOpenFlags::NO_DOTGIT
            | git2::RepositoryOpenFlags::BARE
    }

    pub fn open(loc: path::PathBuf) -> Result<Option<git2::Repository>> {
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
    pub fn is_symlink(&self) -> Result<bool> {
        Ok(err_at!(IOError, fs::metadata(&self.loc))?.is_symlink())
    }

    fn set_age(&mut self, hot: Option<usize>, cold: Option<usize>) {
        let hot = hot.map(|hot| 3600 * 24 * 30 * hot as i64); // in seconds;
        let cold = cold.map(|cold| 3600 * 24 * 30 * cold as i64); // in seconds;

        let now = chrono::offset::Local::now().naive_local();
        let dt = chrono::NaiveDateTime::from_timestamp(self.last_commit_date, 0);

        self.age = match hot {
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
    }
}

#[derive(Clone)]
struct WalkState {
    scan_dir: path::PathBuf,
    opts: Opt,
    repos: Vec<Repo>,
}

impl PrettyRow for Repo {
    fn to_format() -> prettytable::format::TableFormat {
        *prettytable::format::consts::FORMAT_CLEAN
    }

    fn to_head() -> prettytable::Row {
        row![Fy => "Dir", "Commit", "State", "Branches"]
    }

    fn to_row(&self) -> prettytable::Row {
        let (state, attention) = display_repository_state(self);
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

        let color = match (attention, self.age) {
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

        row![dir.color(color), commit_date.color(color), state, brs,]
    }
}

fn display_repository_state(repo: &Repo) -> (String, bool) {
    let mut states = vec![];
    let mut attention = false;

    {
        if repo.is_bare {
            states.push("⛼".to_string().yellow());
        }
    }

    {
        let s: colored::ColoredString = match &repo.state {
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
        let mods = display_modifed(&repo.deltas);
        attention = attention || mods.iter().any(|s| !s.is_plain());
        states.extend_from_slice(&mods)
    }

    {
        if !repo.tags.is_empty() {
            states.push("🏷".magenta());
        }
    }

    {
        if repo.stashs > 0 {
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

fn display_modifed(deltas: &[git2::Delta]) -> Vec<colored::ColoredString> {
    let mut mods = vec![];
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

pub fn handle(opts: Opt) -> Result<()> {
    let state = {
        let mut state = WalkState {
            scan_dir: path::PathBuf::default(),
            opts: opts.clone(),
            repos: vec![],
        };
        for scan_dir in opts.profile.scan_dirs.iter() {
            state.scan_dir = scan_dir.clone();
            let parent = path::Path::new(scan_dir).parent().unwrap();
            make_repo(&mut state, &parent, &files::dir_entry(scan_dir)?)?;
            state = files::walk(scan_dir, state, check_dir_entry)?;
        }
        state
    };

    let index = state
        .repos
        .into_iter()
        .fold(trie::Trie::new(), |mut index, repo| {
            let comps: Vec<ffi::OsString> = path::PathBuf::from(&repo.loc)
                .components()
                .map(|c| c.as_os_str().to_os_string())
                .collect();
            index.set(&comps, repo);
            index
        });

    let mut repos: Vec<Repo> = index
        .walk(Vec::<Repo>::default(), |repos, _, _, value, _, _| {
            value.map(|repo| repos.push(repo.clone()));
            Ok(trie::WalkRes::Ok)
        })?
        .into_iter()
        .filter(|r| !files::is_excluded(&r.loc, &opts.profile.exclude_dirs))
        .collect();

    repos.sort_unstable_by_key(|r| r.last_commit_date);
    repos
        .iter_mut()
        .for_each(|r| r.set_age(opts.profile.hot, opts.profile.cold));

    print::make_table(&repos).print_tty(opts.force_color);

    Ok(())
}

fn check_dir_entry(
    state: &mut WalkState,
    parent: &path::Path,
    entry: &fs::DirEntry,
    _depth: usize,
    _breath: usize,
) -> Result<files::WalkRes> {
    if let Some(".git") = entry.file_name().to_str() {
        Ok(files::WalkRes::SkipDir)
    } else {
        make_repo(state, parent, entry)?;
        Ok(files::WalkRes::Ok)
    }
}

fn make_repo(
    state: &mut WalkState,
    parent: &path::Path,
    entry: &fs::DirEntry,
) -> Result<()> {
    let loc: path::PathBuf = vec![parent.to_path_buf(), entry.file_name().into()]
        .into_iter()
        .collect();

    if let Some(mut repo) = Repo::open(loc.clone())? {
        let (branches, branch) = get_repo_branches(&repo)?;
        let tags: Vec<String> =
            err_at!(Fatal, repo.tag_names(None), "Branch::tag_names {:?}", loc)?
                .iter()
                .filter_map(|o| o.map(|s| s.to_string()))
                .collect();
        let stashs: usize = {
            let mut n = 0;
            repo.stash_foreach(|_, _, _| {
                n += 1;
                true
            })
            .unwrap();
            n
        };
        let last_commit_date = branches
            .iter()
            .map(|br| br.last_commit_date)
            .max()
            .unwrap_or(0);

        let deltas: Vec<git2::Delta> = if repo.is_bare() {
            vec![]
        } else {
            let mut dopts = crate::repo::make_diff_options(state.opts.ignored);
            let diff =
                err_at!(Fatal, repo.diff_index_to_workdir(None, Some(&mut dopts)))?;
            diff.deltas().map(|d| d.status()).collect()
        };

        let is_bare = repo.is_bare();
        let repo_state = repo.state();
        let repo = Repo {
            loc,
            repo,
            age: Age::Hot,
            is_bare,
            state: repo_state,
            deltas,
            is_staged: true,
            is_sync_remote: true,
            branches,
            branch,
            tags,
            stashs,
            last_commit_date,
        };
        state.repos.push(repo);
    }

    Ok(())
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

fn get_reference_kind(refrn: &git2::Reference) -> String {
    refrn
        .kind()
        .map(|k| k.to_string())
        .unwrap_or_else(|| "".to_string())
}

fn get_reference_shorthand(refrn: &git2::Reference) -> String {
    refrn
        .shorthand()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "".to_string())
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
