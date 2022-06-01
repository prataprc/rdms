use rand::{prelude::random, rngs::StdRng};

use rdms::Result;

use crate::SubCommand;

pub trait Generate<T> {
    fn gen_key(&self, rng: &mut StdRng) -> T;

    fn gen_value(&self, rng: &mut StdRng) -> T;
}

pub struct Opt {
    pub seed: u64,
    pub profile: String,
    pub module: String,
}

impl From<crate::SubCommand> for Opt {
    fn from(subcmd: crate::SubCommand) -> Opt {
        match subcmd {
            SubCommand::Perf { seed, profile, module } => Opt { seed, profile, module },
            _ => unreachable!(),
        }
    }
}

pub fn perf(mut opts: Opt) -> Result<()> {
    if opts.seed == 0 {
        opts.seed = random();
    }

    match opts.module.as_str() {
        "btree" | "btree_map" | "btree-map" => crate::perf_btree_map::perf(opts).unwrap(),
        "llrb" => crate::perf_llrb::perf(opts)?,
        "lmdb" => crate::perf_lmdb::perf(opts)?,
        "robt" => crate::perf_robt::perf(opts)?,
        "wral" => crate::perf_wral::perf(opts)?,
        module => println!("rdms: error invalid module {}", module),
    }

    Ok(())
}
