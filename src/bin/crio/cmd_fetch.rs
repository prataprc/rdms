use serde::Deserialize;
use structopt::StructOpt;

use std::{ffi, result};

use rdms::util;

#[derive(Clone, StructOpt)]
struct Opt {
    #[structopt(long = "git")]
    git_root: Option<ffi::OsString>,

    #[structopt(short = "c")]
    profile: ffi::OsString,
}

#[derive(Clone, Deserialize)]
pub struct Profile {
    dump_url: url::Url,
    git_root: ffi::OsString,
    index_dir: ffi::OsString,
    analytics: ffi::OsString,
}

pub fn handle(args: Vec<ffi::OsString>) -> result::Result<(), String> {
    let mut opts = Opt::from_iter(args.clone().into_iter());

    let mut profile: Profile =
        util::files::load_toml(&opts.profile).map_err(|e| e.to_string())?;

    //let mut fd = fs::OpenOptions::new()
    //    .read(true)
    //    .open("/media/prataprc/hdd1.4tb/crates-io/2021-11-09-020028/data/crates.csv")
    //    .unwrap();
    //let mut rdr = csv::Reader::from_reader(&mut fd);
    //for (i, result) in rdr.deserialize().enumerate() {
    //    // Notice that we need to provide a type hint for automatic deserialization.
    //    let record: Crate = result.unwrap();
    //    println!("{:?}", record);
    //    if i > 2 {
    //        return;
    //    }
    //}

    Ok(())
}

#[derive(Deserialize)]
struct Category {
    category: String,
    crates_cnt: String,
    created_at: String,
    description: String,
    id: String,
    path: String,
    slug: String,
}

#[derive(Debug, Deserialize)]
struct Crate {
    created_at: String,
    description: String,
    documentation: String,
    downloads: String,
    homepage: String,
    id: String,
    max_upload_size: String,
    name: String,
    readme: String,
    repository: String,
    updated_at: String,
}
