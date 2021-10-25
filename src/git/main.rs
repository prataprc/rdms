use git2::Repository;
use structopt::StructOpt;

use std::{ffi, process, str::FromStr};

pub use gitkv::{err_at, Error, Result};

mod files;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "gitkv", version = "0.0.1")]
pub struct Opt {
    #[structopt(long = "discover")]
    discover: Option<ffi::OsString>,

    #[structopt(long = "excludes", default_value = "")]
    excludes: Regexs,

    #[structopt(long = "includes", default_value = "")]
    includes: Regexs,
}

#[derive(Debug, Clone)]
struct Regexs(Vec<regex::Regex>);

impl FromStr for Regexs {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut rs = vec![];
        for r in s.split(",").filter(|x| x.len() > 0) {
            rs.push(err_at!(Invalid, regex::Regex::new(r))?);
        }
        Ok(Regexs(rs))
    }
}

impl AsRef<[regex::Regex]> for Regexs {
    fn as_ref(&self) -> &[regex::Regex] {
        &self.0
    }
}

fn main() {
    let opts = Opt::from_args();

    if let Some(path) = opts.discover.clone() {
        process::exit(do_discover(path, opts));
    };
}

fn do_discover(path: ffi::OsString, opts: Opt) -> i32 {
    use gitkv::GIT_DIR;

    const BARE: &'static str = "ᵇ";

    let state = ();
    let res = files::walk(path, state, |_, entry, _depth, _breath| {
        let path = entry.path();
        let loc = path.to_str().unwrap();

        let mut skip = entry.file_name() == GIT_DIR;
        skip = skip || !opts.includes.as_ref().iter().all(|r| r.find(loc).is_some());
        skip = skip || opts.excludes.as_ref().iter().any(|r| r.find(loc).is_some());

        if !skip {
            if let Ok(r) = Repository::open(entry.path()) {
                let mut out = format!("{:?}", entry.path());
                if r.is_bare() {
                    out.push_str(BARE)
                }
                println!("{}", out)
            }
        }

        Ok(())
    });
    match res {
        Ok(_) => return 0,
        Err(err) => {
            println!("{}", err);
            return 1;
        }
    }
}
