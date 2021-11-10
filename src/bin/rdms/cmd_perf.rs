use rand::{prelude::random, rngs::SmallRng};
use structopt::StructOpt;

pub trait Generate<T> {
    fn gen_key(&self, rng: &mut SmallRng) -> T;

    fn gen_value(&self, rng: &mut SmallRng) -> T;
}

/// Options for `perf` subcommand.
#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "seed", default_value = "0")]
    pub seed: u128,

    #[structopt(long = "profile", default_value = "")]
    pub profile: String,

    #[structopt(short = "m", long = "module", default_value = "llrb")]
    pub module: String,
}

pub fn perf(args: Vec<String>) {
    let mut opts = Opt::from_iter(args.clone().into_iter());

    if opts.seed == 0 {
        opts.seed = random();
    }

    match opts.module.as_str() {
        "btree" | "btree_map" | "btree-map" => crate::perf_btree_map::perf(opts).unwrap(),
        "llrb" => crate::perf_llrb::perf(opts).unwrap(),
        "lmdb" => crate::perf_lmdb::perf(opts).unwrap(),
        "robt" => crate::perf_robt::perf(opts).unwrap(),
        "wral" => crate::perf_wral::perf(opts).unwrap(),
        module => println!("rdms: error invalid module {}", module),
    }
}
