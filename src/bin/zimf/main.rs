use structopt::StructOpt;

use std::{ffi, io::Write, sync::Arc, thread, time};

use rdms::zimf::{self, Zimf};

mod print;

#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "info")]
    info: bool,

    #[structopt(long = "urls")]
    urls: bool,

    #[structopt(long = "url")]
    url: Option<String>,

    #[structopt(long = "namespace")]
    namespace: Option<zimf::Namespace>,

    #[structopt(long = "namespaces")]
    namespaces: bool,

    #[structopt(long = "dump")]
    dump: Option<String>,

    #[structopt(long = "dump-all")]
    dump_all: bool,

    #[structopt(long = "color")]
    color: bool,

    #[structopt(long = "json")]
    json: bool,

    #[structopt(long = "threads")]
    pool_size: Option<usize>,

    zim_file: ffi::OsString,
}

fn main() {
    let opts = Opt::from_args();

    let mut z = Zimf::open(opts.zim_file.clone()).unwrap();
    if let Some(pool_size) = opts.pool_size {
        z.set_pool_size(pool_size).unwrap();
    }

    if opts.info && opts.json {
        println!("{}", z.to_json());
    } else if opts.info {
        print::make_info_table(&z).print_tty(opts.color);
        println!();
        print::make_header_table(z.as_header()).print_tty(opts.color);
        println!();
        print::make_mimes_table(&z).print_tty(opts.color);
        println!();
        print::make_namespace_table(&z).print_tty(opts.color);
    }

    let entries = z.as_entries();

    if opts.namespaces {
        let mut namespaces: Vec<zimf::Namespace> =
            entries.iter().map(|e| e.to_namespace().unwrap()).collect();
        namespaces.dedup();
        println!("Namespaces: {:?}", namespaces);
    } else if opts.urls {
        let entries: Box<dyn Iterator<Item = &Arc<zimf::Entry>>> =
            if let Some(namespace) = opts.namespace {
                Box::new(
                    entries
                        .iter()
                        .filter(move |e| namespace == e.to_namespace().unwrap()),
                )
            } else {
                Box::new(entries.iter().filter(|_| true))
            };

        for entry in entries {
            let title = if entry.title.is_empty() {
                "-"
            } else {
                &entry.title
            };
            println!("{}/{:-30} ({})", entry.namespace as char, entry.url, title);
        }
    }

    if let Some(url) = opts.url {
        match entries.binary_search_by_key(&url, |e| e.url.clone()) {
            Ok(n) => {
                let entry = z.get_entry(n).as_ref().clone();
                print::make_entry_table(&entry, &z).print_tty(opts.color);
            }
            Err(_) => {
                println!("Missing url {:?}", url);
            }
        }
    }

    if let Some(url) = opts.dump {
        match entries.binary_search_by_key(&url, |e| e.url.clone()) {
            Ok(n) => {
                let (_entry, data) = z.get_entry_content(n).unwrap();
                debug_assert!(data.len() == std::io::stdout().write(&data).unwrap());
            }
            Err(_) => {
                println!("Missing url {:?}", url);
            }
        }
    }

    if opts.dump_all {
        let z = Arc::new(z);
        let clusters = z.as_clusters();

        let mut handles = vec![];
        let offs: Vec<usize> = (0..clusters.len()).collect();
        for offs in offs.chunks(clusters.len() / 32) {
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
}

#[allow(dead_code)]
fn load_clusters(z: &Zimf) {
    for cnum in 0..z.as_clusters().len() {
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
