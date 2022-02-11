use colored::Colorize;
use prettytable::{cell, row};

use std::{convert::TryFrom, path};

use crate::{h, Config, Handler, SubCommand};

use rdms::{
    git::repo,
    util::print::{self, PrettyRow},
    Error, Result,
};

#[derive(Clone)]
pub struct Handle {
    pub scan_dirs: Vec<path::PathBuf>,
    pub exclude_dirs: Vec<path::PathBuf>,
    pub hot: Option<usize>,
    pub cold: Option<usize>,
    pub ignored: bool,
    pub force_color: bool,
    pub states: bool,
}

impl TryFrom<crate::SubCommand> for Handle {
    type Error = Error;

    fn try_from(subcmd: crate::SubCommand) -> Result<Handle> {
        let opt = match subcmd {
            SubCommand::Status {
                scan_dir,
                ignored,
                force_color,
                states,
            } => Handle {
                scan_dirs: scan_dir.map(|d| vec![d.into()]).unwrap_or_else(|| vec![]),
                exclude_dirs: Vec::default(),
                hot: None,
                cold: None,
                ignored,
                force_color,
                states,
            },
            _ => unreachable!(),
        };

        Ok(opt)
    }
}

impl Handler for Handle {
    fn to_scan_dirs(&self) -> Vec<path::PathBuf> {
        self.scan_dirs.to_vec()
    }

    fn to_exclude_dirs(&self) -> Vec<path::PathBuf> {
        self.exclude_dirs.to_vec()
    }
}

impl Handle {
    fn update_with_cfg(mut self, cfg: &Config) -> Self {
        if let None = self.hot {
            self.hot = cfg.hot;
        }
        if let None = self.cold {
            self.cold = cfg.cold;
        }
        self.scan_dirs.extend_from_slice(&cfg.scan.scan_dirs);
        self.exclude_dirs.extend_from_slice(&cfg.scan.exclude_dirs);

        self
    }
}

#[derive(Copy, Clone, Debug)]
pub enum Age {
    Hot,
    Cold,
    Frozen,
}

pub fn handle(mut h: Handle, cfg: Config) -> Result<()> {
    h = h.update_with_cfg(&cfg);

    if h.states {
        println!("{} clean", "👍".green());
        println!("{} bare repo", "⛼".to_string().yellow());
        println!("{} repo with tags", "🏷".magenta());
        println!("{} repo with stashed changes", "🛍".blue());
        println!("{} edited repo, pending commit", "✎".red());
        println!("{} repo not clean", "☕".red());
        println!("{} repo index/work-tree not sync", "🗳".red());
    } else {
        let mut statuss: Vec<Status> = h::WalkState::new(h.clone())
            .scan()?
            .into_repositories()?
            .into_iter()
            .map(|r| Status::from_opts(&h, r))
            .collect();

        print::make_table(&mut statuss).print_tty(h.force_color);
    }

    Ok(())
}

struct Status {
    hot: Option<usize>,  // in months
    cold: Option<usize>, // in months
    ignored: bool,

    repo: repo::Repo,
}

impl Status {
    fn from_opts(h: &Handle, repo: repo::Repo) -> Status {
        Status {
            hot: h.hot,
            cold: h.cold,
            ignored: h.ignored,

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
