use rand::{prelude::random, rngs::SmallRng, Rng};
use structopt::StructOpt;

use std::{convert::TryFrom, result};

mod btree_map;
mod llrb;
mod lmdb;

/// Command line options.
#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "seed", default_value = "0")]
    seed: u128,

    #[structopt(long = "profile", default_value = "")]
    profile: String,

    command: String,
}

fn main() {
    let mut opts = Opt::from_args();
    if opts.seed == 0 {
        opts.seed = random();
    }

    match opts.command.as_str() {
        "llrb" => llrb::perf(opts).unwrap(),
        "btree" | "btree_map" | "btree-map" => btree_map::perf(opts).unwrap(),
        "lmdb" => lmdb::perf(opts).unwrap(),
        command => println!("rdms-perf: error invalid command {}", command),
    }
}

fn load_profile(opts: &Opt) -> result::Result<toml::Value, String> {
    use std::{fs, str::from_utf8};

    let ppath = opts.profile.clone();
    let s = from_utf8(&fs::read(ppath).expect("invalid profile file path"))
        .expect("invalid profile-text encoding, must be in toml")
        .to_string();
    Ok(s.parse().expect("invalid profile format"))
}

trait Generate<K> {
    fn gen(&self, rng: &mut SmallRng) -> K;
}

#[derive(Clone)]
enum Key {
    U64,
    String(usize),
}

impl Default for Key {
    fn default() -> Key {
        Key::U64
    }
}

impl TryFrom<String> for Key {
    type Error = String;

    fn try_from(s: String) -> result::Result<Key, String> {
        match s.to_lowercase().as_str() {
            "u64" => Ok(Key::U64),
            "string" => Ok(Key::String(16)),
            s => Err(format!("invalid key-type:{:?}", s)),
        }
    }
}

impl Generate<u64> for Key {
    fn gen(&self, rng: &mut SmallRng) -> u64 {
        match self {
            Key::U64 => rng.gen::<u64>(),
            _ => unreachable!(),
        }
    }
}

impl Generate<String> for Key {
    fn gen(&self, rng: &mut SmallRng) -> String {
        let val = rng.gen::<u64>();
        match self {
            Key::String(size) => format!("{:0width$}", val, width = size),
            _ => unreachable!(),
        }
    }
}

impl Key {
    fn to_type(&self) -> &'static str {
        match self {
            Key::U64 => "u64",
            Key::String(_) => "string",
        }
    }
}

#[derive(Clone)]
enum Value {
    U64,
    String(usize),
}

impl Default for Value {
    fn default() -> Value {
        Value::U64
    }
}

impl TryFrom<String> for Value {
    type Error = String;

    fn try_from(s: String) -> result::Result<Value, String> {
        match s.to_lowercase().as_str() {
            "u64" => Ok(Value::U64),
            "string" => Ok(Value::String(16)),
            s => Err(format!("invalid key-type:{:?}", s)),
        }
    }
}

impl Generate<u64> for Value {
    fn gen(&self, rng: &mut SmallRng) -> u64 {
        match self {
            Value::U64 => rng.gen::<u64>(),
            _ => unreachable!(),
        }
    }
}

impl Generate<String> for Value {
    fn gen(&self, rng: &mut SmallRng) -> String {
        let val = rng.gen::<u64>();
        match self {
            Value::String(size) => format!("{:0width$}", val, width = size),
            _ => unreachable!(),
        }
    }
}

impl Value {
    fn to_type(&self) -> &'static str {
        match self {
            Value::U64 => "u64",
            Value::String(_) => "string",
        }
    }
}

#[macro_export]
macro_rules! get_property {
    ($value:ident, $name:expr, $meth:ident, $def:expr) => {
        $value
            .as_table()
            .unwrap()
            .get($name)
            .map(|v| v.$meth().unwrap_or($def))
            .unwrap_or($def)
    };
}
