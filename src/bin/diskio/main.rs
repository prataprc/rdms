use std::{
    convert::TryInto,
    ffi, fs,
    io::{self, Read, Seek, Write},
    path, result,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    thread, time,
};

use rand::{rngs::StdRng, Rng, SeedableRng};
use regex::Regex;
use structopt::StructOpt;
#[macro_use]
extern crate lazy_static;

use rdms::{err_at, Error, Result};

mod plot;
mod stats;

use crate::stats::Stats;

#[derive(Debug, StructOpt, Clone)]
struct Opt {
    path: String,

    #[structopt(long = "block-size", default_value = "1024")]
    block_size: SizeArg,

    #[structopt(long = "duration", default_value = "10")]
    duration: u64,

    #[structopt(long = "appenders", default_value = "0")]
    appenders: isize,

    #[structopt(long = "writers", default_value = "0")]
    writers: isize,

    #[structopt(long = "rangers", default_value = "0")]
    rangers: isize,

    #[structopt(long = "reverses", default_value = "0")]
    reverses: isize,

    #[structopt(long = "readers", default_value = "0")]
    readers: isize,

    #[structopt(long = "seed", default_value = "0")]
    seed: u64,

    #[structopt(long = "plot")]
    plot: bool,
}

impl Opt {
    fn nappenders(&self) -> isize {
        let xs = vec![
            1,
            self.appenders,
            self.writers,
            self.rangers,
            self.reverses,
            self.readers,
        ];
        xs.into_iter().max().unwrap()
    }

    fn nwriters(&self) -> isize {
        self.appenders + self.writers
    }

    fn nreaders(&self) -> isize {
        self.rangers + self.reverses + self.readers
    }

    fn append_block_size(&self, block_size: isize) -> isize {
        if self.appenders == 0 {
            10 * 1024 * 1024
        } else {
            block_size
        }
    }
}

struct Context {
    opt: Opt,
    filename: ffi::OsString,
    fd: fs::File,
    block: Vec<u8>,
    duration: time::Duration,
}

impl Context {
    fn new_append(i: isize, bsize: isize, opt: Opt) -> Context {
        let filename = Context::new_data_file(i, &opt).unwrap();
        println!("creating file `{}` ..", filename.to_str().unwrap());
        let fd = fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(filename.clone())
            .unwrap();
        let duration = time::Duration::from_nanos(opt.duration * 1_000_000_000);

        Context {
            opt,
            fd,
            filename,
            block: {
                let mut block = Vec::with_capacity(bsize as usize);
                block.resize(block.capacity(), 0xAB);
                block
            },
            duration,
        }
    }

    fn new_write(i: isize, bsize: isize, opt: Opt) -> Context {
        let filename = Context::open_data_file(i, &opt).unwrap();
        let fd = fs::OpenOptions::new().write(true).open(filename.clone()).unwrap();
        let duration = time::Duration::from_nanos(opt.duration * 1_000_000_000);

        Context {
            opt,
            fd,
            filename,
            block: {
                let mut block = Vec::with_capacity(bsize as usize);
                block.resize(block.capacity(), 0xAB);
                block
            },
            duration,
        }
    }

    fn new_read(i: isize, bsize: isize, opt: Opt) -> Context {
        let filename = Context::open_data_file(i, &opt).unwrap();
        let fd = fs::OpenOptions::new().read(true).open(filename.clone()).unwrap();
        let duration = time::Duration::from_nanos(opt.duration * 1_000_000_000);

        Context {
            opt,
            fd,
            filename,
            block: {
                let mut block = Vec::with_capacity(bsize as usize);
                block.resize(block.capacity(), 0xAB);
                block
            },
            duration,
        }
    }

    fn new_data_file(id: isize, opt: &Opt) -> io::Result<ffi::OsString> {
        // create dir
        let mut p = path::PathBuf::new();
        p.push(&opt.path);
        fs::create_dir_all(p.as_path())?;
        // remove file
        p.push(format!("diskio-{}.data", id));
        fs::remove_file(p.as_path()).ok();
        Ok(p.into())
    }

    fn open_data_file(id: isize, opt: &Opt) -> io::Result<ffi::OsString> {
        // create dir
        let mut p = path::PathBuf::new();
        p.push(&opt.path);
        p.push(format!("diskio-{}.data", id));
        Ok(p.into())
    }

    fn drop_data_file(id: isize, opt: &Opt) {
        let mut p = path::PathBuf::new();
        p.push(&opt.path);
        p.push(format!("diskio-{}.data", id));
        fs::remove_file(p.as_path()).ok();
    }
}

impl Context {
    fn path_latency_plot(opt: &Opt, block_size: isize) -> path::PathBuf {
        let mut p = path::PathBuf::new();
        p.push(&opt.path);
        p.push(format!(
            "diskio-plot-latency-{}Rx{}Wx{}x{}.png",
            opt.nreaders(),
            opt.nwriters(),
            humanize(block_size.try_into().unwrap()),
            opt.duration,
        ));
        p
    }

    fn path_throughput_plot(opt: &Opt, block_size: isize) -> path::PathBuf {
        let mut p = path::PathBuf::new();
        p.push(&opt.path);
        p.push(format!(
            "diskio-plot-throughput-{}Rx{}Wx{}x{}.png",
            opt.nreaders(),
            opt.nwriters(),
            humanize(block_size.try_into().unwrap()),
            opt.duration,
        ));
        p
    }
}

static W_TOTAL: AtomicU64 = AtomicU64::new(0);
static R_TOTAL: AtomicU64 = AtomicU64::new(0);

fn main() {
    let opt = Opt::from_args();

    for bsize in opt.clone().block_size.get_blocks() {
        // io: append data
        let mut threads = vec![];
        let start_time = time::SystemTime::now();
        let append_bsize = opt.append_block_size(bsize);
        for i in 0..opt.nappenders() {
            let ctxt = Context::new_append(i, append_bsize, opt.clone());
            threads.push(thread::spawn(move || append_thread(i, ctxt)));
        }
        let ss = aggregate_threads(threads);
        log_details(append_bsize, start_time, &ss);
        do_plot(append_bsize, &opt, ss);
        W_TOTAL.store(0, Ordering::Relaxed);

        // io: other operations
        let mut threads = vec![];
        let start_time = time::SystemTime::now();
        for i in 0..opt.writers {
            let ctxt = Context::new_write(i, bsize, opt.clone());
            threads.push(thread::spawn(move || writer_thread(i, ctxt)));
        }
        for i in 0..opt.rangers {
            let ctxt = Context::new_read(i, bsize, opt.clone());
            threads.push(thread::spawn(move || range_thread(i, ctxt)));
        }
        for i in 0..opt.reverses {
            let ctxt = Context::new_read(i, bsize, opt.clone());
            threads.push(thread::spawn(move || reverse_thread(i, ctxt)));
        }
        for i in 0..opt.readers {
            let ctxt = Context::new_read(i, bsize, opt.clone());
            threads.push(thread::spawn(move || reader_thread(i, ctxt)));
        }
        let ss = aggregate_threads(threads);
        log_details(bsize, start_time, &ss);
        do_plot(bsize, &opt, ss);
        W_TOTAL.store(0, Ordering::Relaxed);
        R_TOTAL.store(0, Ordering::Relaxed);

        // remove files
        (0..opt.nappenders()).for_each(|i| Context::drop_data_file(i, &opt));

        println!("");
    }
}

fn aggregate_threads(threads: Vec<thread::JoinHandle<Result<Stats>>>) -> Stats {
    let mut aggr_stats = Stats::new();
    for (i, thread) in threads.into_iter().enumerate() {
        match thread.join() {
            Ok(res) => match res {
                Ok(stat) => aggr_stats.join(stat),
                Err(err) => println!("thread {} errored: {}", i, err),
            },
            Err(_) => println!("thread {} paniced", i),
        }
    }
    aggr_stats
}

fn log_details(bsize: isize, start: time::SystemTime, _ss: &Stats) {
    let elapsed = start.elapsed().expect("failed to compute elapsed");
    let w_total: usize = W_TOTAL.load(Ordering::Relaxed).try_into().unwrap();
    let r_total: usize = R_TOTAL.load(Ordering::Relaxed).try_into().unwrap();

    if w_total > 0 {
        println!(
            "wrote {}, using {} blocks in {:?}",
            humanize(w_total),
            humanize(bsize.try_into().unwrap()),
            elapsed,
            // humanize(_ss.file_size.try_into().unwrap()),
        );
    }
    if r_total > 0 {
        println!(
            "readr {}, using {} blocks in {:?}",
            humanize(r_total),
            humanize(bsize.try_into().unwrap()),
            elapsed,
            // humanize(_ss.file_size.try_into().unwrap()),
        );
    }
}

fn do_plot(bsize: isize, opt: &Opt, ss: Stats) {
    if opt.plot {
        plot::latency(
            Context::path_latency_plot(&opt, bsize),
            format!(
                "fd.sync_all() latency, block-size:{}, wr:{}, rd:{}",
                humanize(bsize.try_into().unwrap()),
                opt.nwriters(),
                opt.nreaders(),
            ),
            ss.sync_latencies,
        )
        .expect("unable to plot latency");

        plot::throughput(
            Context::path_throughput_plot(&opt, bsize),
            format!(
                "throughput for block-size:{}, wr:{}, rd:{}",
                humanize(bsize.try_into().unwrap()),
                opt.nwriters(),
                opt.nreaders(),
            ),
            ss.throughputs,
        )
        .expect("unable to plot latency");
    }
}

fn append_thread(_id: isize, mut ctxt: Context) -> Result<Stats> {
    // println!("append_thread {}", _id);
    let mut ss = Stats::new();
    let block_size: isize = ctxt.block.len().try_into().unwrap();
    let start_time = time::SystemTime::now();
    while start_time.elapsed().unwrap() < ctxt.duration {
        let lbegin = time::SystemTime::now();
        let n = err_at!(IOError, ctxt.fd.write(ctxt.block.as_slice()))?;
        if n != ctxt.block.len() {
            err_at!(IOError, msg: "partial write {}", n)?;
        }
        err_at!(IOError, ctxt.fd.sync_all())?;
        W_TOTAL.fetch_add(block_size.try_into().unwrap(), Ordering::Relaxed);
        ss.click(lbegin, ctxt.block.len().try_into().unwrap())?;
    }

    ss.file_size = err_at!(IOError, fs::metadata(ctxt.filename))?.len();
    Ok(ss)
}

fn writer_thread(id: isize, mut ctxt: Context) -> Result<Stats> {
    // println!("writer_thread {}", id);
    let mut rng = StdRng::seed_from_u64(ctxt.opt.seed + (id as u64));

    let mut ss = Stats::new();
    let file_size = err_at!(IOError, ctxt.fd.metadata())?.len();
    let block_size: isize = ctxt.block.len().try_into().unwrap();
    let start_time = time::SystemTime::now();
    while start_time.elapsed().unwrap() < ctxt.duration {
        let fpos = {
            let scale: f64 = rng.gen_range(0.0..1.0);
            ((file_size as f64) * scale) as u64
        };
        err_at!(IOError, ctxt.fd.seek(io::SeekFrom::Start(fpos)))?;

        let lbegin = time::SystemTime::now();
        let n = err_at!(IOError, ctxt.fd.write(ctxt.block.as_slice()))?;
        if n != ctxt.block.len() {
            err_at!(IOError, msg: "partial write {}", n)?;
        }
        err_at!(IOError, ctxt.fd.sync_all())?;
        W_TOTAL.fetch_add(block_size.try_into().unwrap(), Ordering::Relaxed);
        ss.click(lbegin, ctxt.block.len().try_into().unwrap())?;
    }

    ss.file_size = err_at!(IOError, fs::metadata(ctxt.filename))?.len();
    Ok(ss)
}

fn range_thread(_id: isize, mut ctxt: Context) -> Result<Stats> {
    let mut ss = Stats::new();
    let mut fpos = 0;
    let file_size = err_at!(IOError, ctxt.fd.metadata())?.len();
    let n: u64 = ctxt.block.len().try_into().unwrap();
    let start_time = time::SystemTime::now();
    while start_time.elapsed().unwrap() < ctxt.duration {
        fpos = (fpos + n) % file_size;
        err_at!(IOError, ctxt.fd.seek(io::SeekFrom::Start(fpos)))?;

        let lbegin = time::SystemTime::now();
        let n: u64 = err_at!(IOError, ctxt.fd.read(ctxt.block.as_mut_slice()))?
            .try_into()
            .unwrap();
        R_TOTAL.fetch_add(n, Ordering::Relaxed);
        ss.click(lbegin, n.try_into().unwrap())?;
    }

    ss.file_size = err_at!(IOError, fs::metadata(ctxt.filename))?.len();
    Ok(ss)
}

fn reverse_thread(_id: isize, mut ctxt: Context) -> Result<Stats> {
    let mut ss = Stats::new();
    let file_size = err_at!(IOError, ctxt.fd.metadata())?.len();
    let n: u64 = ctxt.block.len().try_into().unwrap();
    let mut fpos = file_size - n;
    let start_time = time::SystemTime::now();
    while start_time.elapsed().unwrap() < ctxt.duration {
        fpos = (fpos - n) % file_size;
        err_at!(IOError, ctxt.fd.seek(io::SeekFrom::Start(fpos)))?;

        let lbegin = time::SystemTime::now();
        let n: u64 = err_at!(IOError, ctxt.fd.read(ctxt.block.as_mut_slice()))?
            .try_into()
            .unwrap();
        R_TOTAL.fetch_add(n, Ordering::Relaxed);
        ss.click(lbegin, n.try_into().unwrap())?;
    }

    ss.file_size = err_at!(IOError, fs::metadata(ctxt.filename))?.len();
    Ok(ss)
}

fn reader_thread(id: isize, mut ctxt: Context) -> Result<Stats> {
    let mut rng = StdRng::seed_from_u64(ctxt.opt.seed + (id as u64));

    let mut ss = Stats::new();
    let file_size = err_at!(IOError, ctxt.fd.metadata())?.len();
    let start_time = time::SystemTime::now();
    while start_time.elapsed().unwrap() < ctxt.duration {
        let fpos = {
            let scale: f64 = rng.gen_range(0.0..1.0);
            ((file_size as f64) * scale) as u64
        };
        err_at!(IOError, ctxt.fd.seek(io::SeekFrom::Start(fpos)))?;

        let lbegin = time::SystemTime::now();
        let n: u64 = err_at!(IOError, ctxt.fd.read(ctxt.block.as_mut_slice()))?
            .try_into()
            .unwrap();
        R_TOTAL.fetch_add(n, Ordering::Relaxed);
        ss.click(lbegin, n.try_into().unwrap())?;
    }

    ss.file_size = err_at!(IOError, fs::metadata(ctxt.filename))?.len();
    Ok(ss)
}

fn humanize(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < (1024 * 1024) {
        format!("{}KB", bytes / 1024)
    } else if bytes < (1024 * 1024 * 1024) {
        format!("{}MB", bytes / (1024 * 1024))
    } else if bytes < (1024 * 1024 * 1024 * 1024) {
        format!("{}GB", bytes / (1024 * 1024 * 1024))
    } else {
        format!("{}TB", bytes / (1024 * 1024 * 1024 * 1024))
    }
}

#[derive(Debug, Clone)]
enum SizeArg {
    None,
    Range(Option<isize>, Option<isize>),
    List(Vec<isize>),
}

lazy_static! {
    static ref ARG_RE1: Regex = {
        let patt = r"^([0-9]+[kKmMgG]?)(\.\.[0-9]+[kKmMgG]?)?$";
        Regex::new(patt).unwrap()
    };
    static ref ARG_RE2: Regex = {
        let patt = r"^([0-9]+[kKmMgG]?)(,[0-9]+[kKmMgG]?)*$";
        Regex::new(patt).unwrap()
    };
    static ref BLOCK_SIZES: [isize; 9] = [
        128,
        256,
        512,
        1024,
        10 * 1024,
        100 * 1024,
        1024 * 1024,
        10 * 1024 * 1024,
        100 * 1024 * 1024,
    ];
    static ref DATA_SIZES: [isize; 6] = [
        1 * 1024 * 1024,
        10 * 1024 * 1024,
        100 * 1024 * 1024,
        1024 * 1024 * 1024,
        10 * 1024 * 1024 * 1024,
        100 * 1024 * 1024 * 1024,
    ];
}

impl FromStr for SizeArg {
    type Err = String;

    fn from_str(s: &str) -> result::Result<SizeArg, Self::Err> {
        //println!("re1 {}", s);
        match ARG_RE1.captures(s) {
            None => (),
            Some(captrs) => {
                let x = captrs.get(1).map(|m| SizeArg::to_isize(m.as_str()));
                let y = captrs.get(2).map(|m| {
                    let s = m.as_str().chars().skip(2).collect::<String>();
                    SizeArg::to_isize(s.as_str())
                });
                // println!("re1 {}, {:?} {:?}", s, x, y);
                return Ok(SizeArg::Range(x.transpose()?, y.transpose()?));
            }
        };
        //println!("re2 {}", s);
        match ARG_RE2.captures(s) {
            None => Ok(SizeArg::None),
            Some(captrs) => {
                let sizes = captrs
                    .get(0)
                    .unwrap()
                    .as_str()
                    .split(',')
                    .map(|s| SizeArg::to_isize(s).unwrap())
                    .collect::<Vec<isize>>();
                return Ok(SizeArg::List(sizes));
            }
        }
    }
}

impl SizeArg {
    fn to_isize(s: &str) -> result::Result<isize, String> {
        let chs: Vec<char> = s.chars().collect();
        let (s, amp) = match chs[chs.len() - 1] {
            'k' | 'K' => {
                let s: String = chs[..(chs.len() - 1)].iter().collect();
                (s, 1024)
            }
            'm' | 'M' => {
                let s: String = chs[..(chs.len() - 1)].iter().collect();
                (s, 1024 * 1024)
            }
            'g' | 'G' => {
                let s: String = chs[..(chs.len() - 1)].iter().collect();
                (s, 1024 * 1024 * 1024)
            }
            't' | 'T' => {
                let s: String = chs[..(chs.len() - 1)].iter().collect();
                (s, 1024 * 1024 * 1024 * 1024)
            }
            _ => {
                let s: String = chs[..chs.len()].iter().collect();
                (s, 1)
            }
        };
        // println!("{}", s);
        match s.parse::<isize>() {
            Err(err) => Err(format!("parse: {:?}", err)),
            Ok(n) => Ok(n * amp),
        }
    }

    fn get_blocks(self) -> Vec<isize> {
        let (from, till) = match self {
            SizeArg::None => return vec![],
            SizeArg::List(sizes) => return sizes,
            SizeArg::Range(None, None) => return vec![],
            SizeArg::Range(Some(x), None) => return vec![x],
            SizeArg::Range(None, Some(_)) => unreachable!(),
            SizeArg::Range(Some(x), Some(y)) => (x, y),
        };
        BLOCK_SIZES
            .clone()
            .iter()
            .skip_while(|x| **x < from)
            .take_while(|x| **x <= till)
            .map(|x| *x)
            .collect()
    }
}
