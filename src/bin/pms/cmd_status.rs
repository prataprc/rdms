use colored::Colorize;
use prettytable::{cell, row};
use serde::Deserialize;

use std::{convert::TryFrom, ffi, fs, path};

use crate::SubCommand;

use rdms::{
    git::repo,
    trie,
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
pub enum Age {
    Hot,
    Cold,
    Frozen,
}

#[derive(Clone)]
struct WalkState {
    scan_dir: path::PathBuf,
    opts: Opt,
    repos: Vec<repo::Repo>,
}

pub fn handle(opts: Opt) -> Result<()> {
    let walk_state = {
        let mut ws = WalkState {
            scan_dir: path::PathBuf::default(),
            opts: opts.clone(),
            repos: vec![],
        };
        for scan_dir in opts.profile.scan_dirs.iter() {
            ws.scan_dir = scan_dir.clone();
            {
                if let Ok(repo) = repo::Repo::from_loc(scan_dir) {
                    ws.repos.push(repo);
                }
            }
            ws = match files::walk(scan_dir, ws.clone(), check_dir_entry) {
                Ok(ws) => ws,
                Err(err) => {
                    println!("scan_dir {:?}, err:{} skipping ...", scan_dir, err);
                    ws
                }
            };
        }
        ws
    };

    let index =
        walk_state
            .repos
            .into_iter()
            .fold(trie::Trie::new(), |mut index, repo| {
                let comps: Vec<ffi::OsString> = path::PathBuf::from(&repo.to_loc())
                    .components()
                    .map(|c| c.as_os_str().to_os_string())
                    .collect();
                index.set(&comps, repo);
                index
            });

    let mut repos: Vec<repo::Repo> = index
        .walk(Vec::<repo::Repo>::default(), |repos, _, _, value, _, _| {
            value.map(|repo| repos.push(repo.clone()));
            Ok(trie::WalkRes::Ok)
        })?
        .into_iter()
        .filter(|r| !files::is_excluded(&r.to_loc(), &opts.profile.exclude_dirs))
        .collect();

    repos.sort_unstable_by_key(|r| r.to_last_commit_date(None).unwrap());

    let mut srepos: Vec<Status> = repos
        .into_iter()
        .map(|r| Status::from_opts(&opts, r))
        .collect();
    print::make_table(&mut srepos).print_tty(opts.force_color);

    Ok(())
}

fn check_dir_entry(
    walk_state: &mut WalkState,
    parent: &path::Path,
    entry: &fs::DirEntry,
    _depth: usize,
    _breath: usize,
) -> Result<files::WalkRes> {
    if let Some(".git") = entry.file_name().to_str() {
        Ok(files::WalkRes::SkipDir)
    } else {
        if let Ok(repo) = repo::Repo::from_entry(parent, entry) {
            walk_state.repos.push(repo);
        }
        Ok(files::WalkRes::Ok)
    }
}

struct Status {
    hot: Option<usize>,  // in months
    cold: Option<usize>, // in months
    ignored: bool,

    repo: repo::Repo,
}

impl Status {
    fn from_opts(opts: &Opt, repo: repo::Repo) -> Status {
        Status {
            hot: opts.profile.hot,
            cold: opts.profile.cold,
            ignored: opts.ignored,

            repo,
        }
    }
}

impl PrettyRow for Status {
    fn to_format() -> prettytable::format::TableFormat {
        *prettytable::format::consts::FORMAT_CLEAN
    }

    fn to_head() -> prettytable::Row {
        row![Fy => "Dir", "Commit", "State", "Branches", "Remotes"]
    }

    fn to_row(&self) -> prettytable::Row {
        let (display_state, attention) = display_repository_state(self);
        let brs = display_branches(self).unwrap();

        let commit_date = self
            .repo
            .to_last_commit_date(None)
            .unwrap()
            .map(|dt| dt.format("%Y-%b-%d").to_string())
            .unwrap_or("".to_string());

        let dir = {
            let p = self.repo.to_loc();
            let comps = p.components().collect::<Vec<path::Component>>();
            let p = comps
                .into_iter()
                .rev()
                .take(2)
                .rev()
                .collect::<path::PathBuf>();
            p.as_os_str()
                .to_str()
                .map(|s| s.to_string())
                .unwrap_or("--".to_string())
        };

        let remotes = self.repo.to_remote_names().unwrap();

        let age = to_age(&self.repo, self.hot, self.cold).unwrap();
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
            remotes.join(","),
        ]
    }
}

fn display_repository_state(status: &Status) -> (String, bool) {
    let mut states = vec![];
    let mut attention = false;

    {
        if status.repo.is_bare() {
            states.push("⛼".to_string().yellow());
        }
    }

    {
        let s: colored::ColoredString = match &status.repo.to_repo_state() {
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
        let mods = display_modifed(&status);
        attention = attention || mods.iter().any(|s| !s.is_plain());
        states.extend_from_slice(&mods)
    }

    {
        let tags = status.repo.to_tags().unwrap();
        if !tags.is_empty() {
            states.push("🏷".magenta());
        }
    }

    {
        match status.repo.to_references() {
            Ok(refns) => {
                let stash = refns
                    .into_iter()
                    .any(|refn| matches!(refn.shorthand(), Some("stash")));
                if stash {
                    states.push("🛍".blue())
                }
            }
            _ => (),
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

fn display_modifed(status: &Status) -> Vec<colored::ColoredString> {
    let mut mods = vec![];
    let deltas = status.repo.to_deltas(status.ignored).unwrap();

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

    {
        let modf = status
            .repo
            .to_statuses(None)
            .unwrap()
            .into_iter()
            .any(|s| !matches!(s, git2::Status::CURRENT | git2::Status::IGNORED));
        if modf {
            mods.push("🗳".red())
        }
    }

    mods
}

fn display_branches(status: &Status) -> Option<String> {
    let branches = status
        .repo
        .to_branches(Some(git2::BranchType::Local))
        .ok()?;

    let hname = status
        .repo
        .to_current_branch()
        .ok()
        .flatten()
        .map(|br| repo::Repo::branch_name(&br));

    let s = branches
        .into_iter()
        .filter_map(|br| {
            let name = repo::Repo::branch_name(&br);
            let ups = status
                .repo
                .branch_upstream(br)
                .map(|(ups, s)| (repo::Repo::branch_name(&ups), s));

            match (ups, hname.as_ref()) {
                (Some((un, synced)), Some(hn)) if &name == hn && synced => {
                    Some(format!("{} <-> {}", name, un).yellow())
                }
                (Some((un, _)), Some(hn)) if &name == hn => {
                    Some(format!("{} <-> {}", name, un).cyan())
                }
                (Some((_, synced)), _) if synced => None,
                (Some((un, _)), _) => Some(format!("{} <-> {}", name, un).cyan()),
                (None, _) => Some(format!("{}", name).white()),
            }
        })
        .map(|s| s.to_string())
        .collect::<Vec<String>>()
        .join("\n");
    Some(s)
}

fn to_age(repo: &repo::Repo, hot: Option<usize>, cold: Option<usize>) -> Result<Age> {
    let hot = hot.map(|hot| 3600 * 24 * 30 * hot as i64); // in seconds;
    let cold = cold.map(|cold| 3600 * 24 * 30 * cold as i64); // in seconds;

    let now = chrono::offset::Local::now().naive_local();
    let dt = repo.to_last_commit_date(None)?;

    let age = match (hot, dt) {
        (Some(s), Some(dt)) if dt < (now - chrono::Duration::seconds(s)) => match cold {
            Some(s) if dt < (now - chrono::Duration::seconds(s)) => Age::Frozen,
            Some(_) | None => Age::Cold,
        },
        (Some(_), Some(_)) => Age::Hot,
        (Some(_), None) => Age::Hot,
        (None, Some(dt)) => match cold {
            Some(secs) if dt < (now - chrono::Duration::seconds(secs)) => Age::Cold,
            Some(_) | None => Age::Hot,
        },
        (None, None) => Age::Hot,
    };

    Ok(age)
}
