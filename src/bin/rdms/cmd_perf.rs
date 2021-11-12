use rand::{prelude::random, rngs::SmallRng};

use rdms::Result;

use crate::SubCommand;

pub trait Generate<T> {
    fn gen_key(&self, rng: &mut SmallRng) -> T;

    fn gen_value(&self, rng: &mut SmallRng) -> T;
}

pub struct Opt {
    pub seed: u128,
    pub profile: String,
    pub module: String,
}

impl From<crate::SubCommand> for Opt {
    fn from(subcmd: crate::SubCommand) -> Opt {
        match subcmd {
            SubCommand::Perf {
                seed,
                profile,
                module,
            } => Opt {
                seed,
                profile,
                module,
            },
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
