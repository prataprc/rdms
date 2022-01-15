#![feature(is_symlink)]

use structopt::StructOpt;

use std::{convert::TryFrom, ffi};

use rdms::Result;

mod cmd_status;
mod repo;

/// Options for cmd
#[derive(Clone, StructOpt)]
pub struct Opt {
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
        loc: Option<ffi::OsString>,

        #[structopt(
            long = "ignored",
            help = "included ignored files in git2::DiffOptions"
        )]
        ignored: bool,

        #[structopt(long = "force_color", help = "force color for non-terminal devices")]
        force_color: bool,

        #[structopt(
            long = "toml",
            help = "Location to config file for processing git repositories"
        )]
        toml: Option<ffi::OsString>,
    },
}

fn main() {
    let opts = Opt::from_iter(std::env::args_os());

    let res = handle_subcmd(opts);
    res.map_err(|e| println!("Error: {}", e)).ok();
}

fn handle_subcmd(opts: Opt) -> Result<()> {
    match opts.subcmd {
        c @ SubCommand::Status { .. } => {
            cmd_status::handle(cmd_status::Opt::try_from(c)?)
        }
    }
}
