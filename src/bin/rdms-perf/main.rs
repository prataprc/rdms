use rand::{prelude::random, rngs::SmallRng};
use structopt::StructOpt;

use std::result;

mod btree_map;
mod llrb;
mod lmdb;
mod robt;

/// Command line options.
#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "seed", default_value = "0")]
    seed: u128,

    #[structopt(long = "profile", default_value = "")]
    profile: String,

    command: String,
}

fn main() {
    let mut opts = Opt::from_args();
    if opts.seed == 0 {
        opts.seed = random();
    }

    match opts.command.as_str() {
        "btree" | "btree_map" | "btree-map" => btree_map::perf(opts).unwrap(),
        "llrb" => llrb::perf(opts).unwrap(),
        "lmdb" => lmdb::perf(opts).unwrap(),
        //"robt" => robt::perf(opts).unwrap(),
        command => println!("rdms-perf: error invalid command {}", command),
    }
}

fn load_profile(opts: &Opt) -> result::Result<String, String> {
    use std::{fs, str::from_utf8};

    let ppath = opts.profile.clone();
    let s = from_utf8(&fs::read(ppath).expect("invalid profile file path"))
        .expect("invalid profile-text encoding, must be in toml")
        .to_string();
    Ok(s)
    // Ok(s.parse().expect("invalid profile format"))
}

trait Generate<T> {
    fn gen_key(&self, rng: &mut SmallRng) -> T;

    fn gen_value(&self, rng: &mut SmallRng) -> T;
}
