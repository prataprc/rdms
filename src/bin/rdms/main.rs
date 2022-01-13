#![feature(is_symlink)]

use structopt::StructOpt;

use std::{convert::TryFrom, ffi};

use rdms::Result;

mod cmd_git;
mod cmd_lgit;
mod cmd_perf;

mod perf_btree_map;
mod perf_llrb;
mod perf_lmdb;
mod perf_robt;
mod perf_wral;

/// Options for cmd
#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clone, StructOpt)]
pub enum SubCommand {
    /// perf-subcommand, to execute a performance profile, to measure algorithms.
    Perf {
        #[structopt(long = "seed", default_value = "0")]
        seed: u64,

        #[structopt(long = "profile", default_value = "")]
        profile: String,

        #[structopt(short = "m", long = "module", default_value = "llrb")]
        module: String,
    },
    /// git-subcommand, to play with git and dba systems.
    Git {
        #[structopt(long = "repo", help = "location of git repository")]
        loc_repo: Option<ffi::OsString>,

        #[structopt(
            long = "db",
            help = "db-path within git repository, refer <loc_repo>"
        )]
        loc_db: Option<ffi::OsString>,

        #[structopt(long = "sha1-file", help = "generate SHA1 hash for file's content")]
        sha1_file: Option<ffi::OsString>,

        #[structopt(long = "sha1", help = "generate SHA1 hash for text")]
        sha1_text: Option<String>,
    },
    /// lgit subcommand, to find local git repositories under home directory.
    Lgit {
        #[structopt(
            long = "path",
            help = "root path to start looking for git repositories"
        )]
        path: Option<ffi::OsString>,

        #[structopt(
            long = "follow-link",
            help = "follow symbolic links, by default sym-links are skipped "
        )]
        sym_link: bool,

        #[structopt(
            long = "toml",
            help = "Configuration file for processing git repositories"
        )]
        toml: Option<String>,
    },
}

fn main() {
    let opts = Opt::from_iter(std::env::args_os());

    let res = handle_subcmd(opts);
    res.map_err(|e| println!("Error: {}", e)).ok();
}

fn handle_subcmd(opts: Opt) -> Result<()> {
    match opts.subcmd {
        c @ SubCommand::Perf { .. } => cmd_perf::perf(cmd_perf::Opt::from(c)),
        c @ SubCommand::Git { .. } => cmd_git::handle(cmd_git::Opt::from(c)),
        c @ SubCommand::Lgit { .. } => cmd_lgit::handle(cmd_lgit::Opt::try_from(c)?),
    }
}
