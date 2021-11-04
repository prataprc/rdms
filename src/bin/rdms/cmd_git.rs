use structopt::StructOpt;

use std::ffi;

use rdms::{git, Result};

/// Options for `git` subcommand.
#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "db")]
    pub db: Option<ffi::OsString>,

    loc: ffi::OsString,
}

pub fn handle(args: Vec<String>) -> Result<()> {
    let mut opts = Opt::from_iter(args.clone().into_iter());

    let config = git::Config {
        loc_repo: opts.loc.clone(),
        loc_db: opts.db.unwrap_or(opts.loc.clone()),
        permissions: None,
        description: "git command".to_string(),
    };
    let index = git::Index::open(config.clone())?;

    println!("{}", index.len().unwrap());

    Ok(())
}
