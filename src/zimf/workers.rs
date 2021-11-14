use rand;

use std::{convert::TryFrom, ffi, fs, panic, sync::mpsc, thread};

use crate::{zimf::Cluster, Error, Result};

pub enum Req {
    ClusterHeader {
        off: u64,
        tx: mpsc::Sender<Result<Res>>,
    },
    ClusterBlocks {
        cluster: Cluster,
        tx: mpsc::Sender<Result<Res>>,
    },
}

pub enum Res {
    Cluster { cluster: Cluster },
    Blocks { blobs: Vec<Vec<u8>> },
}

impl TryFrom<Res> for Cluster {
    type Error = Error;

    fn try_from(res: Res) -> Result<Cluster> {
        match res {
            Res::Cluster { cluster } => Ok(cluster),
            _ => unreachable!(),
        }
    }
}

pub struct Workers {
    _zim_loc: ffi::OsString,
    t_pool: Vec<thread::JoinHandle<Result<()>>>,
    tx_pool: Vec<mpsc::SyncSender<Req>>,
}

impl Drop for Workers {
    fn drop(&mut self) {
        use std::mem;

        for tx in self.tx_pool.drain(..) {
            mem::drop(tx)
        }
        for handle in self.t_pool.drain(..) {
            match handle.join() {
                Ok(Ok(())) => (),
                Ok(Err(err)) => panic!("Workers::drop:{}", err),
                Err(err) => panic::resume_unwind(err),
            }
        }
    }
}

impl Workers {
    pub fn new_pool(zim_loc: ffi::OsString, n_threads: usize) -> Result<Workers> {
        let (mut t_pool, mut tx_pool) = (vec![], vec![]);
        for id in 0..n_threads {
            let fd = err_at!(IOError, fs::OpenOptions::new().read(true).open(&zim_loc))?;
            let zim_loc = zim_loc.clone();
            let (tx, rx) = mpsc::sync_channel(16);
            let handler = thread::spawn(move || worker(id, zim_loc, fd, rx));

            t_pool.push(handler);
            tx_pool.push(tx);
        }

        let val = Workers {
            _zim_loc: zim_loc,
            t_pool,
            tx_pool,
        };

        Ok(val)
    }

    pub fn read_cluster_header(
        &self,
        off: u64, // cluster start fpos
        tx: mpsc::Sender<Result<Res>>,
    ) -> Result<()> {
        let t = rand::random::<usize>() % self.tx_pool.len();
        err_at!(
            IPCFail,
            self.tx_pool[t].send(Req::ClusterHeader { off, tx })
        )
    }

    pub fn read_cluster_blobs(
        &self,
        cluster: Cluster, // cluster start fpos
        tx: mpsc::Sender<Result<Res>>,
    ) -> Result<()> {
        let t = rand::random::<usize>() % self.tx_pool.len();
        err_at!(
            IPCFail,
            self.tx_pool[t].send(Req::ClusterBlocks { cluster, tx })
        )
    }
}

fn worker(
    _id: usize,
    _zim_loc: ffi::OsString,
    mut fd: fs::File,
    rx: mpsc::Receiver<Req>,
) -> Result<()> {
    for msg in rx {
        // println!("worker id:{} received msg", _id);
        match msg {
            Req::ClusterHeader { off, tx } => match Cluster::from_offset(off, &mut fd) {
                Ok(cluster) => err_at!(IPCFail, tx.send(Ok(Res::Cluster { cluster })))?,
                Err(err) => err_at!(IPCFail, tx.send(Err(err)))?,
            },
            Req::ClusterBlocks { cluster, tx } => {
                // println!("worker id:{} cluster {:?}", _id, cluster);
                match cluster.to_blobs(&mut fd) {
                    Ok(blobs) => err_at!(IPCFail, tx.send(Ok(Res::Blocks { blobs })))?,
                    Err(err) => err_at!(IPCFail, tx.send(Err(err)))?,
                }
            }
        }
    }
    // println!("worker id:{} closed", _id);

    Ok(())
}
