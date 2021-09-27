use rand::prelude::random;
use structopt::StructOpt;

mod llrb;

/// Command line options.
#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "seed", default_value = "0")]
    seed: u128,

    #[structopt(long = "key_type", default_value = "u64")]
    key_type: String,

    #[structopt(long = "spin")]
    spin: bool,

    #[structopt(long = "loads", default_value = "1000000")] // default 1M
    loads: usize,

    #[structopt(long = "sets", default_value = "0")] // default 1M
    sets: usize,

    #[structopt(long = "ins", default_value = "0")] // default 1M
    ins: usize,

    #[structopt(long = "rems", default_value = "0")] // default 1M
    rems: usize,

    #[structopt(long = "dels", default_value = "0")] // default 1M
    dels: usize,

    #[structopt(long = "cas")]
    cas: bool,

    #[structopt(long = "gets", default_value = "0")] // default 1M
    gets: usize,

    #[structopt(long = "writers", default_value = "1")]
    writers: usize,

    #[structopt(long = "readers", default_value = "1")]
    readers: usize,

    // can be one of llrb
    command: String,
}

impl Opt {
    fn reset_writeops(&mut self) {
        self.sets = 0;
        self.ins = 0;
        self.rems = 0;
        self.dels = 0;
    }

    fn reset_readops(&mut self) {
        self.gets = 0;
    }
}

fn main() {
    let mut opts = Opt::from_args();
    if opts.seed == 0 {
        opts.seed = random();
    }

    match (opts.command.as_str(), opts.key_type.as_str()) {
        ("llrb", "u64") => llrb::perf::<u64>(opts).unwrap(),
        (command, _) => println!("rdms-perf: error invalid command {}", command),
    }
}
