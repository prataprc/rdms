use std::{
    ffi, fmt, fs,
    ops::Bound,
    result,
    sync::{self},
};

use log::info;

use crate::{
    core::{Diff, DiskIndexFactory, Footprint, Index, IndexIter, PiecewiseScan},
    core::{Result, Serialize, WriteIndexFactory},
    lsm,
    scans::{self, SkipScan},
    types::Empty,
};

#[derive(Clone)]
struct Name(String);

impl From<String> for Name {
    fn from(name: String) -> Name {
        Name(format!("{}-backup", name))
    }
}

impl From<Name> for String {
    fn from(name: Name) -> String {
        let parts: Vec<&str> = name.0.split('-').collect();
        let name = parts[..(parts.len() - 1)].join("-");
        name
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

pub struct Backup<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    dir: ffi::OsString,
    name: String,
    pw_batch: usize,

    mem: M::I,
    disk: sync::Mutex<Option<D::I>>,
}

impl<K, V, M, D> Backup<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    pub fn new(
        dir: &ffi::OsStr, // directory path
        name: &str,
        mem_factory: M,
        disk_factory: D,
    ) -> Result<Backup<K, V, M, D>> {
        let mem = mem_factory.new(name)?;
        let disk = disk_factory.new(dir, name)?;

        Ok(Backup {
            dir: dir.to_os_string(),
            name: name.to_string(),
            pw_batch: scans::SKIP_SCAN_BATCH_SIZE,

            mem,
            disk: sync::Mutex::new(Some(disk)),
        })
    }

    pub fn open(
        dir: &ffi::OsStr, // directory path
        name: &str,
        mem_factory: M,
        disk_factory: D,
    ) -> Result<Backup<K, V, M, D>> {
        let mut items = fs::read_dir(dir)?;
        let disk = loop {
            match items.next() {
                Some(item) => {
                    let item = item?;
                    let mf = item.file_name();
                    let disk = disk_factory.open(dir, mf.clone().into())?;
                    if disk.to_name() == name {
                        break Some(disk);
                    }
                }
                None => break None,
            }
        };
        match disk {
            None => Self::new(dir, name, mem_factory, disk_factory),
            Some(disk) => {
                let mem = mem_factory.new(name)?;
                Ok(Backup {
                    dir: dir.to_os_string(),
                    name: name.to_string(),
                    pw_batch: scans::SKIP_SCAN_BATCH_SIZE,

                    mem,
                    disk: sync::Mutex::new(Some(disk)),
                })
            }
        }
    }

    pub fn set_pw_batch_size(&mut self, batch: usize) {
        self.pw_batch = batch
    }
}

impl<K, V, M, D> Backup<K, V, M, D>
where
    K: Clone + Ord + Footprint,
    V: Clone + Diff + Footprint,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn disk_footprint(&self) -> Result<isize> {
        let disk = self.disk.lock().unwrap();
        disk.as_ref().unwrap().footprint()
    }

    fn mem_footprint(&self) -> Result<isize> {
        self.mem.footprint()
    }
}

impl<K, V, M, D> Footprint for Backup<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint + From<<V as Diff>::D>,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
{
    fn footprint(&self) -> Result<isize> {
        Ok(self.disk_footprint()? + self.mem_footprint()?)
    }
}

impl<K, V, M, D> Index<K, V> for Backup<K, V, M, D>
where
    K: Clone + Ord + Serialize + Footprint,
    V: Clone + Diff + Serialize + Footprint + From<<V as Diff>::D>,
    <V as Diff>::D: Serialize,
    M: WriteIndexFactory<K, V>,
    D: DiskIndexFactory<K, V>,
    <M::I as Index<K, V>>::R: PiecewiseScan<K, V>,
{
    type W = <M::I as Index<K, V>>::W;
    type R = <M::I as Index<K, V>>::R;
    type O = Empty;

    fn to_name(&self) -> String {
        self.name.clone()
    }

    fn to_root(&self) -> Empty {
        Empty
    }

    fn to_metadata(&mut self) -> Result<Vec<u8>> {
        let mut disk = self.disk.lock().unwrap();
        disk.as_mut().unwrap().to_metadata()
    }

    fn to_seqno(&mut self) -> u64 {
        self.mem.to_seqno()
    }

    fn set_seqno(&mut self, seqno: u64) {
        self.mem.set_seqno(seqno)
    }

    fn to_writer(&mut self) -> Result<Self::W> {
        self.mem.to_writer()
    }

    fn to_reader(&mut self) -> Result<Self::R> {
        self.mem.to_reader()
    }

    fn commit(mut self, iter: IndexIter<K, V>, meta: Vec<u8>) -> Result<Self> {
        {
            let mut guard = self.disk.lock().unwrap();
            let mut disk = guard.take().unwrap();
            let within = (
                Bound::Included(self.mem.to_seqno()),
                Bound::Excluded(disk.to_seqno()),
            );
            let mut pw_iter = SkipScan::new(self.mem.to_reader()?, within);
            pw_iter.set_batch_size(self.pw_batch);
            let no_reverse = false;
            let iter = lsm::y_iter(iter, Box::new(pw_iter), no_reverse);
            guard.get_or_insert(disk.commit(iter, meta)?);
        }
        Ok(self)
    }

    fn compact(self) -> Result<Self> {
        {
            let mut guard = self.disk.lock().unwrap();
            let disk = guard.take().unwrap();
            guard.get_or_insert(disk.compact()?);
        }
        Ok(self)
    }
}

#[cfg(test)]
#[path = "dgm_test.rs"]
mod dgm_test;
