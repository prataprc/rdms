use structopt::StructOpt;

use std::{ffi, str::from_utf8, sync::Arc, thread, time};

use rdms::zimf::Zimf;

mod print;

#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "info")]
    info: bool,

    #[structopt(long = "urls")]
    urls: bool,

    #[structopt(long = "url")]
    url: Option<String>,

    #[structopt(short = "n")]
    n: Option<usize>,

    #[structopt(long = "dump")]
    dump: bool,

    #[structopt(long = "dump-all")]
    dump_all: bool,

    #[structopt(long = "color")]
    color: bool,

    #[structopt(long = "json")]
    json: bool,

    #[structopt(long = "threads", default_value = "64")]
    pool_size: usize,

    zim_file: ffi::OsString,
}

fn main() {
    let mut opts = Opt::from_args();

    let z = Zimf::open(opts.zim_file.clone(), opts.pool_size).unwrap();

    if opts.info && opts.json {
        println!("{}", z.to_json());
    } else if opts.info {
        print::make_info_table(&z).print_tty(opts.color);
        println!();
        print::make_header_table(&z.header).print_tty(opts.color);
        println!();
        print::make_mimes_table(&z).print_tty(opts.color);
        println!();
        print::make_namespace_table(&z).print_tty(opts.color);
    }

    if opts.urls {
        for entry in z.entries.iter() {
            println!("{}/{:-30}", entry.namespace as char, entry.url);
            if !entry.title.is_empty() {
                println!("    {}", entry.title);
            }
        }
    }

    opts.n = match opts.url {
        Some(url) => match z.entries.binary_search_by_key(&url, |e| e.url.clone()) {
            Ok(n) => Some(n),
            Err(_) => {
                println!("Missing url {:?}", url);
                opts.n
            }
        },
        None => opts.n,
    };

    match opts.n {
        Some(n) if opts.dump => {
            let (_entry, data) = z.get_entry_content(n).unwrap();
            println!("{}", from_utf8(&data).unwrap());
        }
        Some(n) => {
            let entry = z.get_entry(n).as_ref().clone();
            print::make_entry_table(&entry, &z).print_tty(opts.color);
        }
        None if opts.dump_all => {
            let z = Arc::new(z);
            let mut handles = vec![];
            let offs: Vec<usize> = (0..z.clusters.len()).collect();
            for offs in offs.chunks(z.clusters.len() / 32) {
                // println!("{:?}", offs);
                let (z, offs) = (Arc::clone(&z), offs.to_vec());
                handles.push(thread::spawn(move || {
                    let mut index = vec![];
                    for c in offs {
                        let blobs = z.get_blobs(c).unwrap();
                        let n = blobs.len();
                        index.push((c, vec![0; n]));
                    }
                    index
                }));
            }

            let mut indices = vec![];
            for handle in handles.into_iter() {
                indices.extend(handle.join().unwrap())
            }
            indices.sort_by_key(|x| x.0);

            let n: usize = indices.iter().map(|x| x.1.len()).sum();
            println!("decompressed {} entries", n);
        }
        None => (),
    }
}

#[allow(dead_code)]
fn load_clusters(z: &Zimf) {
    for cnum in 0..z.clusters.len() {
        let start = time::Instant::now();
        let blobs = z.get_blobs(cnum).unwrap();
        println!(
            "cnum:{} blobs:{} elapsed:{:?}",
            cnum,
            blobs.len(),
            start.elapsed()
        );
    }
}
