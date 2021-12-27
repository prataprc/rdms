use std::{convert::TryFrom, ffi, fs, sync::mpsc};

use crate::{util, zimf::Cluster, Error, Result};

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

pub fn read_cluster_header(
    pool: &util::thread::Pool<Req, Res, Result<()>>,
    off: u64, // cluster start fpos
    tx: mpsc::Sender<Result<Res>>,
) -> Result<()> {
    let req = Req::ClusterHeader { off, tx };
    pool.post(req)
}

pub fn read_cluster_blobs(
    pool: &util::thread::Pool<Req, Res, Result<()>>,
    cluster: Cluster, // cluster start fpos
    tx: mpsc::Sender<Result<Res>>,
) -> Result<()> {
    let req = Req::ClusterBlocks { cluster, tx };
    pool.post(req)
}

pub fn worker(zim_loc: ffi::OsString, rx: util::thread::Rx<Req, Res>) -> Result<()> {
    let mut fd = err_at!(IOError, fs::OpenOptions::new().read(true).open(&zim_loc))?;
    for msg in rx {
        // println!("worker id:{} received msg", _id);
        match msg {
            (Req::ClusterHeader { off, tx }, None) => {
                match Cluster::from_offset(off, &mut fd) {
                    Ok(cluster) => {
                        err_at!(IPCFail, tx.send(Ok(Res::Cluster { cluster })))?
                    }
                    Err(err) => err_at!(IPCFail, tx.send(Err(err)))?,
                }
            }
            (Req::ClusterBlocks { cluster, tx }, None) => {
                // println!("worker id:{} cluster {:?}", _id, cluster);
                match cluster.to_blobs(&mut fd) {
                    Ok(blobs) => err_at!(IPCFail, tx.send(Ok(Res::Blocks { blobs })))?,
                    Err(err) => err_at!(IPCFail, tx.send(Err(err)))?,
                }
            }
            _ => unreachable!(),
        }
    }
    // println!("worker id:{} closed", _id);

    Ok(())
}
