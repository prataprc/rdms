use structopt::StructOpt;

use std::ffi;

mod cmd_fetch;

pub const TEMP_DIR_CRIO: &'static str = "crio";

#[derive(Clone, StructOpt)]
struct Opt {
    #[structopt(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clone, StructOpt)]
pub enum SubCommand {
    Fetch {
        #[structopt(long = "nohttp")]
        nohttp: bool,

        #[structopt(long = "nountar")]
        nountar: bool,

        #[structopt(long = "nocopy")]
        nocopy: bool,

        #[structopt(long = "git")]
        git_root: Option<ffi::OsString>,

        #[structopt(short = "c")]
        profile: ffi::OsString,
    },
}

fn main() {
    let opts = Opt::from_iter(std::env::args_os());

    let res = match opts.subcmd.clone() {
        c @ SubCommand::Fetch { .. } => cmd_fetch::handle(cmd_fetch::Opt::from(c)),
    };

    res.map_err(|e| println!("Error: {}", e)).ok();
}
