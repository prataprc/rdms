use std::{convert::TryFrom, path};

use crate::{h, Config, Handler, SubCommand};

use rdms::{err_at, git::repo, Error, Result};

#[derive(Clone)]
pub struct Handle {
    pub scan_dirs: Vec<path::PathBuf>,
    pub exclude_dirs: Vec<path::PathBuf>,
    pub src_dir: path::PathBuf,
    pub dst_dir: path::PathBuf,
}

impl TryFrom<crate::SubCommand> for Handle {
    type Error = Error;

    fn try_from(subcmd: crate::SubCommand) -> Result<Handle> {
        let opt = match subcmd {
            SubCommand::Clone {
                scan_dir,
                src_dir,
                dst_dir,
            } => Handle {
                scan_dirs: scan_dir.map(|d| vec![d.into()]).unwrap_or_else(|| vec![]),
                exclude_dirs: Vec::default(),
                src_dir: src_dir.into(),
                dst_dir: dst_dir.into(),
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

    let ws = h::WalkState::new(h.clone()).scan()?;
    let scan_dir = ws.to_scan_dir();
    let repos: Vec<repo::Repo> = ws.into_repositories()?;

    for repo in repos.into_iter() {
        let rloc = err_at!(Fatal, repo.to_loc().strip_prefix(&scan_dir))?.to_path_buf();
        let rloc: path::PathBuf = [h.dst_dir.clone(), rloc].iter().collect();
        println!("{:?}", rloc);
    }

    Ok(())
}
