use git2;
use structopt::StructOpt;

/// Options for `git` subcommand.
#[derive(Clone, StructOpt)]
pub struct Opt {
    //
}

pub fn handle(args: Vec<String>) {
    let mut opts = Opt::from_iter(args.clone().into_iter());
    println!("any-{}", git2::ObjectType::Tree);
}
