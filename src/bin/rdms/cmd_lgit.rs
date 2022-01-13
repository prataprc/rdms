use prettytable::{cell, row};
use serde::Deserialize;

use std::{convert::TryFrom, fmt, fs, path, result};

use crate::SubCommand;
use rdms::{
    err_at, trie,
    util::{
        files,
        print::{self, PrettyRow},
    },
    Error, Result,
};

const NOT_A_BRANCH: &'static str = "XXX";

#[derive(Clone)]
pub struct Opt {
    pub path: String,
    pub sym_link: bool,
    pub ignored: bool,
    pub force_color: bool,
    pub toml: Option<String>,
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
    stashs: Vec<String>,
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

    is_head: bool,
    upstream: Option<Box<Branch>>,
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
        row![Fy => "Path", "Dir", "State", "Branch"]
    }

    fn to_row(&self) -> prettytable::Row {
        match (&self.branch, is_modified(&self.deltas)) {
            (None, _) => row![ Fc =>
                self.parent, self.path, repository_state(self.state), NOT_A_BRANCH,
            ],
            (Some(br), Some(modf)) => row![ Fr =>
                self.parent, self.path, repository_state(self.state),
                format!("{} {}", br.name, modf)
            ],
            (Some(br), None) => row![
                self.parent,
                self.path,
                repository_state(self.state),
                format!("{}", br.name)
            ],
        }
    }
}

impl TryFrom<crate::SubCommand> for Opt {
    type Error = Error;

    fn try_from(subcmd: crate::SubCommand) -> Result<Opt> {
        let opt = match subcmd {
            SubCommand::Lgit {
                path,
                sym_link,
                toml,
            } => Opt {
                path: path
                    .map(|path| Some(path.to_str().unwrap().to_string()))
                    .unwrap_or(dirs::home_dir().map(|d| d.to_str().unwrap().to_string()))
                    .ok_or_else(|| {
                        let e: Result<()> =
                            err_at!(Fatal, msg: "missing home directory, supply path");
                        e.unwrap_err()
                    })?,
                sym_link,
                ignored: false,
                force_color: false,
                toml,
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
        let state = WalkState {
            opts: opts.clone(),
            repos: vec![],
        };
        files::walk(&opts.path, state, check_dir_entry)?
    };

    let repos: Vec<Repo> = state.repos.into_iter().filter(|r| !r.sym_link).collect();

    let mut index = trie::Trie::new();
    index = repos.into_iter().fold(index, |mut index, repo| {
        let comps: Vec<Component> = path::PathBuf::from(&repo.path)
            .components()
            .into_iter()
            .map(Component::from)
            .collect();
        index.set(&comps, repo);
        index
    });

    let repo_list = index.walk(Vec::<RepoList>::default(), build_repo_list)?;

    let mut repos = repo_list
        .iter()
        .filter(|r| !opts.profile.exclude_dirs.contains(&r.parent))
        .fold(vec![], |mut acc, rl| {
            acc.extend_from_slice(&rl.repos);
            acc
        });

    repos
        .iter_mut()
        .for_each(|r| r.parent = r.parent.strip_prefix(&opts.path).unwrap().to_string());
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
    use git2::RepositoryOpenFlags;

    if let Some(".git") = entry.file_name().to_str() {
        return Ok(files::WalkRes::SkipDir);
    }

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

    if let Some(repo) = repo {
        let (branches, branch) = get_repo_branches(&repo)?;
        let tags: Vec<String> =
            err_at!(Fatal, repo.tag_names(None), "Branch::tag_names {:?}", path)?
                .iter()
                .filter_map(|o| o.map(|s| s.to_string()))
                .collect();

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
            stashs: vec![],
        };
        state.repos.push(repo);
    }

    Ok(files::WalkRes::Ok)
}

fn get_repo_branches(repo: &git2::Repository) -> Result<(Vec<Branch>, Option<Branch>)> {
    let path = repo.path();

    let (mut branches, mut branch) = (vec![], None);
    for res in err_at!(Fatal, repo.branches(None), "Repository::branches")?.into_iter() {
        let (br, branch_type) = err_at!(Fatal, res, "branch iter for {:?}", path)?;
        let br = get_repo_branch(&path, br, branch_type)?;

        let is_head = {
            let br = err_at!(
                Fatal,
                repo.find_branch(&br.name, branch_type.clone()),
                "branch iter for {:?}",
                path
            )?;
            repo.head()
                .ok()
                .as_ref()
                .map(|h| h.is_branch())
                .unwrap_or(false)
                && br.into_reference() == repo.head().ok().unwrap()
        };

        if is_head {
            branch = Some(br.clone())
        }
        branches.push(br)
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
    let upstream = branch
        .upstream()
        .ok()
        .map(|upstream| get_repo_branch(path, upstream, branch_type).ok())
        .flatten();

    let refrn = branch.into_reference();

    let kind = get_reference_kind(&refrn);
    let shorthand = get_reference_shorthand(&refrn);
    let resolved_kind = refrn
        .resolve()
        .ok()
        .map(|r| get_reference_kind(&r))
        .unwrap_or("".to_string());
    let resolved_shorthand = refrn
        .resolve()
        .ok()
        .map(|r| get_reference_shorthand(&r))
        .unwrap_or("".to_string());
    let target = refrn
        .target()
        .map(|o| o.to_string())
        .unwrap_or("".to_string());

    let branch = Branch {
        name,
        branch_type: branch_type_to_string(&branch_type),

        kind,
        shorthand,
        resolved_kind,
        resolved_shorthand,
        target,

        is_head,
        upstream: upstream.map(Box::new),
    };

    Ok(branch)
}

fn get_reference_kind(refrn: &git2::Reference) -> String {
    refrn
        .kind()
        .map(|k| k.to_string())
        .unwrap_or("".to_string())
}

fn get_reference_shorthand(refrn: &git2::Reference) -> String {
    refrn
        .shorthand()
        .map(|s| s.to_string())
        .unwrap_or("".to_string())
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
        repo.path = comp.to_string().into();
        repo.parent = parent.clone();
        repo
    });

    match value {
        Some(repo) => match rl.binary_search_by(|val| parent.cmp(&val.parent)) {
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
        },
        None => (),
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

fn is_modified(deltas: &[git2::Delta]) -> Option<String> {
    let mut status = "".to_string();
    if deltas.iter().any(|d| matches!(d, git2::Delta::Added)) {
        status.push('A')
    }
    if deltas.iter().any(|d| matches!(d, git2::Delta::Deleted)) {
        status.push('D')
    }
    if deltas.iter().any(|d| matches!(d, git2::Delta::Modified)) {
        status.push('M')
    }
    if deltas.iter().any(|d| matches!(d, git2::Delta::Renamed)) {
        status.push('R')
    }
    if deltas.iter().any(|d| matches!(d, git2::Delta::Copied)) {
        status.push('C')
    }
    if deltas.iter().any(|d| matches!(d, git2::Delta::Ignored)) {
        status.push('I')
    }
    if deltas.iter().any(|d| matches!(d, git2::Delta::Untracked)) {
        status.push('U')
    }
    if deltas.iter().any(|d| {
        matches!(
            d,
            git2::Delta::Typechange | git2::Delta::Unreadable | git2::Delta::Conflicted
        )
    }) {
        status.push('X')
    }

    match status.as_str() {
        "" => None,
        status => Some(status.to_string()),
    }
}

fn repository_state(state: git2::RepositoryState) -> String {
    match state {
        git2::RepositoryState::Clean => "clean",
        git2::RepositoryState::Merge => "merge",
        git2::RepositoryState::Revert => "revert",
        git2::RepositoryState::RevertSequence => "revert-seq",
        git2::RepositoryState::CherryPick => "cherry-pick",
        git2::RepositoryState::CherryPickSequence => "cherry-pick-seq",
        git2::RepositoryState::Bisect => "bisect",
        git2::RepositoryState::Rebase => "rebase",
        git2::RepositoryState::RebaseInteractive => "rebase-interactive",
        git2::RepositoryState::RebaseMerge => "rebase-merge",
        git2::RepositoryState::ApplyMailbox => "apply-mailbox",
        git2::RepositoryState::ApplyMailboxOrRebase => "apply-mailbox/rebase",
    }
    .to_string()
}
