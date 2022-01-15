use colored::Colorize;
use prettytable::{cell, row};
use serde::Deserialize;

use std::{convert::TryFrom, env, fmt, fs, path, result};

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
    pub path: String,
    pub sym_link: bool,
    pub ignored: bool,
    pub force_color: bool,
    pub toml: Option<path::PathBuf>,
    pub profile: Profile, // toml options
}

#[derive(Clone, Default, Deserialize)]
pub struct Profile {
    exclude_dirs: Vec<String>,
}

#[derive(Clone)]
struct Repo {
    parent: String,
    path: String,
    bare: bool,
    sym_link: bool,
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

#[derive(Clone)]
struct Branch {
    name: String,
    branch_type: String, // git2::BranchType,

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

#[derive(Clone)]
struct RepoList {
    parent: String,
    repos: Vec<Repo>,
}

#[derive(Clone)]
struct WalkState {
    opts: Opt,
    repos: Vec<Repo>,
}

impl PrettyRow for Repo {
    fn to_format() -> prettytable::format::TableFormat {
        *prettytable::format::consts::FORMAT_CLEAN
    }

    fn to_head() -> prettytable::Row {
        row![Fy => "Path", "Dir", "Commit", "State", "Branches"]
    }

    fn to_row(&self) -> prettytable::Row {
        let (state, attention) = repository_state(self);
        let brs = branches(self);
        let commit_date = chrono::NaiveDateTime::from_timestamp(self.last_commit_date, 0)
            .format("%Y %b %d");

        match attention {
            true => row![
                self.parent.to_string().red(),
                self.path.to_string().red(),
                commit_date,
                state,
                brs
            ],
            false => row![self.parent, self.path, commit_date, state, brs],
        }
    }
}

impl TryFrom<crate::SubCommand> for Opt {
    type Error = Error;

    fn try_from(subcmd: crate::SubCommand) -> Result<Opt> {
        let opt = match subcmd {
            SubCommand::Status {
                path,
                sym_link,
                ignored,
                force_color,
                toml,
            } => Opt {
                path: path
                    .map(|path| path.to_str().unwrap().to_string())
                    .unwrap_or_else(|| {
                        env::current_dir().unwrap().to_str().unwrap().to_string()
                    }),
                sym_link,
                ignored,
                force_color,
                toml: match toml {
                    toml @ Some(_) => toml.map(path::PathBuf::from),
                    None => files::find_config(&["lgit.toml", ".lgit.toml"]),
                },
                profile: Profile::default(),
            },
            _ => unreachable!(),
        };

        Ok(opt)
    }
}

pub fn handle(mut opts: Opt) -> Result<()> {
    opts.profile = match opts.toml.as_ref() {
        Some(p) => files::load_toml(p)?,
        None => Profile::default(),
    };

    let state = {
        let mut state = WalkState {
            opts: opts.clone(),
            repos: vec![],
        };
        let parent = path::Path::new(&opts.path).parent().unwrap();
        make_repo(&mut state, &parent, &files::dir_entry(&opts.path)?)?;
        files::walk(&opts.path, state, check_dir_entry)?
    };

    let index = state.repos.into_iter().filter(|r| !r.sym_link).fold(
        trie::Trie::new(),
        |mut index, repo| {
            let comps: Vec<Component> = path::PathBuf::from(&repo.path)
                .components()
                .into_iter()
                .map(Component::from)
                .collect();
            index.set(&comps, repo);
            index
        },
    );

    let repo_list = index.walk(Vec::<RepoList>::default(), build_repo_list)?;

    let mut repos = repo_list
        .iter()
        .filter(|r| !opts.profile.exclude_dirs.contains(&r.parent))
        .fold(vec![], |mut acc, rl| {
            acc.extend_from_slice(&rl.repos);
            acc
        });
    repos.sort_unstable_by_key(|r| r.last_commit_date);

    repos.iter_mut().for_each(|r| {
        r.parent = r
            .parent
            .strip_prefix(&opts.path)
            .map(|s| s.to_string())
            .unwrap_or_else(|| "".to_string())
    });
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
    use git2::RepositoryOpenFlags;

    let work_flags = RepositoryOpenFlags::NO_SEARCH | RepositoryOpenFlags::CROSS_FS;
    let bare_flags = RepositoryOpenFlags::NO_SEARCH
        | RepositoryOpenFlags::CROSS_FS
        | RepositoryOpenFlags::NO_DOTGIT
        | RepositoryOpenFlags::BARE;
    let ceiling_dirs = Vec::<String>::default().into_iter();

    let path: path::PathBuf = vec![parent.to_path_buf(), entry.file_name().into()]
        .into_iter()
        .collect();

    let sym_link = err_at!(IOError, entry.metadata())?.is_symlink();

    let repo1 = git2::Repository::open_ext(&path, work_flags, ceiling_dirs.clone());
    let repo2 = git2::Repository::open_ext(&path, bare_flags, ceiling_dirs);

    let (bare, repo) = match repo1 {
        Ok(repo) => (false, Some(repo)),
        Err(_) => match repo2 {
            Ok(repo) => (true, Some(repo)),
            Err(err) if err.code() == git2::ErrorCode::NotFound => (false, None),
            Err(err) => err_at!(Fatal, Err(err))?,
        },
    };

    if let Some(mut repo) = repo {
        let (branches, branch) = get_repo_branches(&repo)?;
        let tags: Vec<String> =
            err_at!(Fatal, repo.tag_names(None), "Branch::tag_names {:?}", path)?
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
            let mut dopts = make_diff_options(&state.opts);
            let diff =
                err_at!(Fatal, repo.diff_index_to_workdir(None, Some(&mut dopts)))?;
            diff.deltas().map(|d| d.status()).collect()
        };

        let repo = Repo {
            parent: "".to_string(),
            path: path.as_os_str().to_str().unwrap().to_string(),
            bare,
            sym_link,
            state: repo.state(),
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
        branch_type: branch_type_to_string(&branch_type),

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

fn branch_type_to_string(bt: &git2::BranchType) -> String {
    match bt {
        git2::BranchType::Local => "local".to_string(),
        git2::BranchType::Remote => "remote".to_string(),
    }
}

fn build_repo_list(
    rl: &mut Vec<RepoList>,
    comps: &[Component],
    comp: &Component,
    value: Option<&Repo>,
    _depth: usize,
    _breath: usize,
) -> Result<trie::WalkRes> {
    let parent: path::PathBuf = comps
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<String>>()
        .into_iter()
        .collect();

    let parent = parent.as_os_str().to_str().unwrap().to_string();

    let value = value.cloned().map(|mut repo| {
        repo.path = comp.to_string();
        repo.parent = parent.clone();
        repo
    });

    if let Some(repo) = value {
        match rl.binary_search_by(|val| parent.cmp(&val.parent)) {
            Ok(off) => {
                rl[off].repos.push(repo);
                rl[off].repos.sort_unstable_by(|a, b| a.path.cmp(&b.path));
            }
            Err(off) => rl.insert(
                off,
                RepoList {
                    parent,
                    repos: vec![repo],
                },
            ),
        }
    }

    Ok(trie::WalkRes::Ok)
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
enum Component {
    Prefix(String),
    RootDir,
    CurDir,
    ParentDir,
    Normal(String),
}

impl<'a> From<path::Component<'a>> for Component {
    fn from(comp: path::Component<'a>) -> Component {
        match comp {
            path::Component::Prefix(val) => {
                Component::Prefix(val.as_os_str().to_str().unwrap().to_string())
            }
            path::Component::RootDir => Component::RootDir,
            path::Component::CurDir => Component::CurDir,
            path::Component::ParentDir => Component::ParentDir,
            path::Component::Normal(val) => {
                Component::Normal(val.to_str().unwrap().to_string())
            }
        }
    }
}

impl fmt::Display for Component {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Component::Prefix(val) => write!(f, "{}", val),
            Component::RootDir => write!(f, "/"),
            Component::CurDir => write!(f, "."),
            Component::ParentDir => write!(f, ".."),
            Component::Normal(val) => write!(f, "{}", val),
        }
    }
}

fn make_diff_options(opts: &Opt) -> git2::DiffOptions {
    let mut dopts = git2::DiffOptions::new();
    dopts
        .include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(opts.ignored)
        .recurse_ignored_dirs(opts.ignored)
        .ignore_filemode(false)
        .ignore_submodules(false)
        .include_unreadable(true);
    dopts
}

fn is_modified(deltas: &[git2::Delta]) -> Vec<colored::ColoredString> {
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

fn repository_state(repo: &Repo) -> (String, bool) {
    let mut states = vec![];
    let mut attention = false;

    {
        if repo.bare {
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
        let mods = is_modified(&repo.deltas);
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

fn branches(repo: &Repo) -> String {
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
