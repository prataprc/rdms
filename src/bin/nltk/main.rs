use structopt::StructOpt;

use std::{convert::TryFrom, ffi, fs};

use rdms::{err_at, Error, Result};

pub const TEMP_DIR_CRIO: &str = "crio";

#[derive(Clone, StructOpt)]
struct Opt {
    #[structopt(long = "words")]
    words: bool,

    #[structopt(long = "zimf")]
    zimf: Option<ffi::OsString>,

    #[structopt(long = "threads", default_value = "64")]
    pool_size: usize,

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

    let res = if let Some(zim_file) => opts.zimf {
        work_zimf(zim_file.clone(), opts)
    };

    match res {
        Ok(()) => (),
        Err(err) => println!("Error: {}", err),
    }
}

fn work_zimf(zim_file: ffi::OsString, opts: Opt) -> Result<()> {
    let z = Zimf::open(zim_file.clone(), opts.pool_size).unwrap();
}

fn dom_list(file: ffi::OsString, _opts: Opt) -> Result<()> {
    let text = {
        let data = err_at!(IOError, fs::read(&file))?;
        let text = err_at!(FailConvert, std::str::from_utf8(&data))?.to_string();
        text.trim().to_string()
    };
    let doc = scraper::html::Html::parse_document(&text);
    let body: scraper::ElementRef = doc
        .root_element()
        .select(&scraper::selector::Selector::try_from("body").unwrap())
        .next()
        .unwrap();

    let text_iter = body.text().filter_map(|t| match t.trim() {
        "" => None,
        s => Some(s),
    });

    for t in text_iter {
        println!("{}", t);
    }

    Ok(())
}
