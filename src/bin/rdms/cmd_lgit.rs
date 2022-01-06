use std::{convert::TryFrom, ffi, fs, path};

use rdms::{err_at, util::files, Error, Result};

use crate::SubCommand;

pub struct Opt {
    pub path: ffi::OsString,
    pub sym_link: bool,
}

struct Repo {
    path: ffi::OsString,
    bare: bool,
    sym_link: bool,
    branches: Vec<Branch>,
    tags: Vec<String>,
}

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

impl TryFrom<crate::SubCommand> for Opt {
    type Error = Error;

    fn try_from(subcmd: crate::SubCommand) -> Result<Opt> {
        let opt = match subcmd {
            SubCommand::Lgit { path, sym_link } => Opt {
                path: path
                    .map(Some)
                    .unwrap_or(dirs::home_dir().map(|d| d.into_os_string()))
                    .ok_or_else(|| {
                        let e: Result<()> =
                            err_at!(Fatal, msg: "missing home directory, supply path");
                        e.unwrap_err()
                    })?,
                sym_link,
            },
            _ => unreachable!(),
        };

        Ok(opt)
    }
}

pub fn handle(opts: Opt) -> Result<()> {
    let mut repos = {
        let repos: Vec<Repo> = vec![];
        files::walk(&opts.path, repos, check_dir_entry)?
    };

    repos = repos.into_iter().filter(|r| !r.sym_link).collect();

    for repo in repos.iter() {
        println!("{:?} bare:{}", repo.path, repo.bare);
    }

    Ok(())
}

fn check_dir_entry(
    repos: &mut Vec<Repo>,
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
        let branches = get_repo_branches(&repo)?;
        let tags: Vec<String> =
            err_at!(Fatal, repo.tag_names(None), "Branch::tag_names {:?}", path)?
                .iter()
                .filter_map(|o| o.map(|s| s.to_string()))
                .collect();
        let repo = Repo {
            path: path.into(),
            bare,
            sym_link,
            branches,
            tags,
        };
        repos.push(repo);
    }

    Ok(files::WalkRes::Ok)
}

fn get_repo_branches(repo: &git2::Repository) -> Result<Vec<Branch>> {
    let path = repo.path();

    let mut branches = vec![];
    for res in err_at!(Fatal, repo.branches(None), "Repository::branches")?.into_iter() {
        let (branch, branch_type) = err_at!(Fatal, res, "branch iter for {:?}", path)?;
        branches.push(get_repo_branch(&path, branch, branch_type)?);
    }

    Ok(branches)
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
