use structopt::StructOpt;

use std::{ffi, fs};

use rdms::{err_at, html, parsec, Error, Result};

pub const TEMP_DIR_CRIO: &str = "crio";

#[derive(Clone, StructOpt)]
struct Opt {
    #[structopt(long = "parsec")]
    parsec: bool,

    file: Option<ffi::OsString>,
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

    let res = if opts.parsec {
        let parser = html::new_parser().unwrap();
        parser.pretty_print("");
        Ok(())
    } else if let Some(file) = opts.file.clone() {
        dom_list(file, opts)
    } else {
        Ok(())
    };

    match res {
        Ok(()) => (),
        Err(err) => println!("Error: {}", err),
    }
}

fn dom_list(file: ffi::OsString, _opts: Opt) -> Result<()> {
    let text = {
        let data = err_at!(IOError, fs::read(&file))?;
        let text = err_at!(FailConvert, std::str::from_utf8(&data))?.to_string();
        html::prepare_text(text)
    };
    let mut lex = parsec::Lex::new(text.to_string());

    let parser = html::new_parser().unwrap();
    let node = html::parse_full(&parser, &mut lex)?.unwrap();

    let dom = match html::Dom::from_node(node) {
        Some(dom) => Ok(dom),
        None => err_at!(InvalidInput, msg: "{:?} is not proper html", file),
    }?;
    dom.pretty_print("", true);
    Ok(())
}
