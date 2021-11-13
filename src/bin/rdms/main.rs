use structopt::StructOpt;

use std::ffi;

mod cmd_git;
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
    Perf {
        #[structopt(long = "seed", default_value = "0")]
        seed: u64,

        #[structopt(long = "profile", default_value = "")]
        profile: String,

        #[structopt(short = "m", long = "module", default_value = "llrb")]
        module: String,
    },
    Git {
        #[structopt(long = "db")]
        loc_db: Option<ffi::OsString>,

        #[structopt(long = "hash-file")]
        hash_file: Option<ffi::OsString>,

        loc_repo: Option<ffi::OsString>,
    },
}

fn main() {
    let opts = Opt::from_iter(std::env::args_os());

    let res = match opts.subcmd {
        c @ SubCommand::Perf { .. } => cmd_perf::perf(cmd_perf::Opt::from(c)),
        c @ SubCommand::Git { .. } => cmd_git::handle(cmd_git::Opt::from(c)),
    };

    res.map_err(|e| println!("Error: {}", e)).ok();
}
