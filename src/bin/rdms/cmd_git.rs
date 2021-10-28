use structopt::StructOpt;

/// Options for `git` subcommand.
#[derive(Clone, StructOpt)]
pub struct Opt {
    //#[structopt(long = "seed", default_value = "0")]
//pub seed: u128,

//#[structopt(long = "profile", default_value = "")]
//pub profile: String,

//#[structopt(short = "m", long = "module", default_value = "llrb")]
//pub module: String,
}

pub fn handle(args: Vec<String>) {
    let mut opts = Opt::from_iter(args.clone().into_iter());
}
