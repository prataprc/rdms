use std::ffi;

use rdms::{git, Result};

use crate::SubCommand;

pub struct Opt {
    pub loc_repo: ffi::OsString,
    pub loc_db: Option<ffi::OsString>,
}

impl From<crate::SubCommand> for Opt {
    fn from(subcmd: crate::SubCommand) -> Opt {
        match subcmd {
            SubCommand::Git { loc_repo, loc_db } => Opt { loc_repo, loc_db },
            _ => unreachable!(),
        }
    }
}

pub fn handle(opts: Opt) -> Result<()> {
    let config = git::Config {
        loc_repo: opts.loc_repo.to_str().unwrap().to_string(),
        loc_db: opts
            .loc_db
            .as_ref()
            .unwrap_or(&opts.loc_repo)
            .to_str()
            .unwrap()
            .to_string(),
        permissions: None,
        description: "git command".to_string(),
    };
    let mut index = git::Index::open(config)?;

    println!("{}", index.len().unwrap());

    let mut txn = index.transaction().unwrap();
    txn.insert("/a", "hello world").unwrap();
    txn.commit().unwrap();

    Ok(())
}
