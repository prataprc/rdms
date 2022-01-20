use structopt::StructOpt;

use std::{convert::TryFrom, ffi};

use rdms::{util::files, Result};

mod cmd_status;
mod config;

use config::{Config, TomlConfig};

/// Options for cmd
#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(
        long = "toml",
        help = "Location to config file for processing git repositories"
    )]
    toml: Option<ffi::OsString>,

    #[structopt(
        long = "db",
        help = "Location to db, where pms database is persisted on disk"
    )]
    db: Option<ffi::OsString>,

    #[structopt(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clone, StructOpt)]
pub enum SubCommand {
    /// Status subcommand, to scan local git repositories.
    Status {
        #[structopt(
            long = "path",
            help = "root path to start looking for git repositories"
        )]
        scan_dir: Option<ffi::OsString>,

        #[structopt(
            long = "ignored",
            help = "included ignored files in git2::DiffOptions"
        )]
        ignored: bool,

        #[structopt(long = "force_color", help = "force color for non-terminal devices")]
        force_color: bool,
    },
}

fn main() {
    let opts = Opt::from_iter(std::env::args_os());

    let res = handle(opts);
    res.map_err(|e| println!("Error: {}", e)).ok();
}

fn handle(opts: Opt) -> Result<()> {
    let cfg: Config = {
        let loc_toml = files::find_config(opts.toml.clone(), &["pms.toml", ".pms.toml"]);
        match loc_toml.as_ref() {
            Some(loc_toml) => files::load_toml::<_, TomlConfig>(loc_toml)?.into(),
            None => Config::default(),
        }
    };

    handle_subcmd(opts, cfg)
}

fn handle_subcmd(opts: Opt, cfg: Config) -> Result<()> {
    match opts.subcmd {
        c @ SubCommand::Status { .. } => {
            cmd_status::handle(cmd_status::Handle::try_from(c)?, cfg)
        }
    }
}