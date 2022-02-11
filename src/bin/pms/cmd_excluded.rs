use std::{convert::TryFrom, path};

use crate::{util, Config, Handler, SubCommand};

use rdms::{git::repo, trie, util::files, Error, Result};

#[derive(Clone)]
pub struct Handle {
    pub scan_dirs: Vec<path::PathBuf>,
    pub exclude_dirs: Vec<path::PathBuf>,
}

impl TryFrom<crate::SubCommand> for Handle {
    type Error = Error;

    fn try_from(subcmd: crate::SubCommand) -> Result<Handle> {
        let opt = match subcmd {
            SubCommand::Excluded { scan_dir } => Handle {
                scan_dirs: scan_dir.map(|d| vec![d.into()]).unwrap_or_else(|| vec![]),
                exclude_dirs: Vec::default(),
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
        self.scan_dirs.extend_from_slice(&cfg.scan.scan_dirs);
        self.exclude_dirs.extend_from_slice(&cfg.scan.exclude_dirs);

        self
    }
}

pub fn handle(mut h: Handle, cfg: Config) -> Result<()> {
    h = h.update_with_cfg(&cfg);

    let index = util::WalkState::new(h.clone()).scan()?.into_trie();

    let mut repos: Vec<repo::Repo> = index
        .walk(Vec::<repo::Repo>::default(), |repos, _, _, value, _, _| {
            value.map(|repo| repos.push(repo.clone()));
            Ok(trie::WalkRes::Ok)
        })?
        .into_iter()
        .filter(|r| files::is_excluded(&r.to_loc(), &h.to_exclude_dirs()))
        .collect();

    repos.sort_unstable_by_key(|r| r.to_last_commit_date(None).unwrap());

    for repo in repos.into_iter() {
        println!("excluded {:?}", repo.to_loc())
    }

    Ok(())
}
