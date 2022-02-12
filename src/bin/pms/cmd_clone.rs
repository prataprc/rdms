use std::{convert::TryFrom, path};

use crate::{h, Config, Handler, SubCommand};

use rdms::{err_at, git::repo, Error, Result};

#[derive(Clone)]
pub struct Handle {
    pub exclude_dirs: Vec<path::PathBuf>,
    pub src_dir: path::PathBuf,
    pub dst_dir: path::PathBuf,
}

impl TryFrom<crate::SubCommand> for Handle {
    type Error = Error;

    fn try_from(subcmd: crate::SubCommand) -> Result<Handle> {
        let opt = match subcmd {
            SubCommand::Clone { src_dir, dst_dir } => Handle {
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
        vec![self.src_dir.clone()]
    }

    fn to_exclude_dirs(&self) -> Vec<path::PathBuf> {
        self.exclude_dirs.to_vec()
    }
}

impl Handle {
    fn update_with_cfg(mut self, cfg: &Config) -> Self {
        self.exclude_dirs.extend_from_slice(&cfg.scan.exclude_dirs);
        self
    }
}

pub fn handle(mut h: Handle, cfg: Config) -> Result<()> {
    h = h.update_with_cfg(&cfg);

    let ws = h::WalkState::new(h.clone()).scan()?;

    let src_dir = ws.to_scan_dir();
    let mut repos: Vec<repo::Repo> = ws.into_repositories()?;

    repos.sort_unstable_by_key(|r| r.to_loc());

    for repo in repos.into_iter() {
        let src_loc = repo.to_loc();
        let dst_loc: path::PathBuf = {
            let dst = err_at!(Fatal, src_loc.strip_prefix(&src_dir))?.to_path_buf();
            [h.dst_dir.clone(), dst].iter().collect()
        };
        repo::clone(src_loc, dst_loc)?;
    }

    Ok(())
}
