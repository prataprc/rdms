use structopt::StructOpt;

use std::{convert::TryFrom, ffi, path};

use rdms::{util::files, Result};

mod cmd_clone;
mod cmd_excluded;
mod cmd_status;
mod config;
mod h;

use config::{Config, TomlConfig};

/// Options for cmd
#[derive(StructOpt)]
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

        #[structopt(long = "states", help = "list states of a repository")]
        states: bool,
    },
    /// Excluded subcommand, to list repositories detected under <path> but excluded.
    Excluded {
        #[structopt(
            long = "path",
            help = "root path to start looking for git repositories"
        )]
        scan_dir: Option<ffi::OsString>,
    },
    /// Clone subcommand, to clone repositories found in <src> to <dst>.
    ///
    /// As and when required new directories shall be created in <dst>.
    Clone {
        #[structopt(long = "src", help = "clone repositories from specified source")]
        src_dir: ffi::OsString,

        #[structopt(long = "dst", help = "clone repositories into specified destin.")]
        dst_dir: ffi::OsString,
    },
}

pub trait Handler {
    fn to_scan_dirs(&self) -> Vec<path::PathBuf>;

    fn to_exclude_dirs(&self) -> Vec<path::PathBuf>;
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
        c @ SubCommand::Excluded { .. } => {
            cmd_excluded::handle(cmd_excluded::Handle::try_from(c)?, cfg)
        }
        c @ SubCommand::Clone { .. } => {
            cmd_clone::handle(cmd_clone::Handle::try_from(c)?, cfg)
        }
    }
}
