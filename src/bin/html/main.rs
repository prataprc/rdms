use structopt::StructOpt;

use std::ffi;

use rdms::html::new_parser;

pub const TEMP_DIR_CRIO: &str = "crio";

#[derive(Clone, StructOpt)]
struct Opt {
    #[structopt(long = "parsec")]
    parsec: bool,
    //#[structopt(subcommand)]
    //subcmd: SubCommand,
}

//#[derive(Clone, StructOpt)]
//pub enum SubCommand {
//    /// Fetch the crates_io dump via http, untar the file and extract the tables.
//    Fetch {
//        #[structopt(long = "nohttp")]
//        nohttp: bool,
//
//        #[structopt(long = "nountar")]
//        nountar: bool,
//
//        #[structopt(long = "nocopy")]
//        nocopy: bool,
//
//        #[structopt(long = "git")]
//        git_root: Option<ffi::OsString>,
//
//        #[structopt(short = "c")]
//        profile: ffi::OsString,
//    },
//}

fn main() {
    let opts = Opt::from_iter(std::env::args_os());

    if opts.parsec {
        let parser = new_parser().unwrap();
        parser.pretty_print("");
    }
}
