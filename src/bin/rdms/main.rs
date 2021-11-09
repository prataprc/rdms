use structopt::StructOpt;

use rdms::util;

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
    //
}

fn main() {
    let (_args, cmd, cmd_args) = util::parse_os_args(None);

    let cmd = cmd.to_str().unwrap().to_string();
    let cmd_args: Vec<String> = cmd_args
        .into_iter()
        .map(|x| x.to_str().unwrap().to_string())
        .collect();

    match cmd.as_str() {
        "perf" => cmd_perf::perf(cmd_args),
        "git" => cmd_git::handle(cmd_args).unwrap(),
        cmd => println!("invalid command {}", cmd),
    }
}
