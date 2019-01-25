use std::fmt::{self, Display};
use std::str::FromStr;
use std::string::ToString;
use std::time::{SystemTime, UNIX_EPOCH};

use rand::{rngs::SmallRng, Rng, SeedableRng};
use structopt::StructOpt;

use bogn::Llrb;

#[derive(Debug)]
enum Error {
    TypeError(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?})", self)
    }
}

#[derive(Debug)]
struct KeyType(String, i32);

impl FromStr for KeyType {
    type Err = Error;

    fn from_str(s: &str) -> Result<KeyType, Error> {
        let mut args: Vec<String> = vec![];
        for item in s.splitn(2, ',') {
            args.push(item.to_string());
        }
        // TODO: Allowed type.
        Ok(KeyType(args[0].clone(), 0))
    }
}

#[derive(Debug, StructOpt)]
struct Opt {
    command: String,
    #[structopt(long = "load", default_value = "10000000")]
    load: u64,
    #[structopt(long = "key", default_value = "i64")]
    key: KeyType,
    #[structopt(long = "value", default_value = "i64")]
    value: KeyType,
    #[structopt(long = "lsm")]
    lsm: bool,
    #[structopt(long = "seed", default_value = "0")]
    seed: u128,
    //#[structopt(long = "ops", default_value = "1000000000")]
    //ops: u64,
}

struct Context {
    opt: Opt,
    rng: SmallRng,
    seed: u128,
}

fn main() {
    let opt = Opt::from_args();
    let (rng, seed) = make_rng(&opt);
    let mut c = Context { opt, rng, seed };

    println!("starting with seed = {}", seed);

    do_perf(&mut c);
}

fn do_perf(c: &mut Context) {
    let start = SystemTime::now();
    let mut numbers: Vec<i64> = Vec::with_capacity(c.opt.load as usize);
    for _i in 0..c.opt.load {
        numbers.push(c.rng.gen());
    }
    println!(
        "generated {} numbers in {:?}",
        numbers.len(),
        start.elapsed().unwrap()
    );

    let start = SystemTime::now();
    let mut llrb: Llrb<i64, i64> = Llrb::new("do-perf", c.opt.lsm);
    for key in numbers.iter() {
        llrb.set(*key, *key);
    }
    println!(
        "loaded `{}` llrb with {} entries in {:?}",
        llrb.id(),
        llrb.count(),
        start.elapsed().unwrap()
    );
}

fn make_rng(opt: &Opt) -> (SmallRng, u128) {
    let seed: u128 = if opt.seed == 0 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    } else {
        opt.seed
    };
    (SmallRng::from_seed(seed.to_le_bytes()), seed)
}
