use std::{sync::mpsc, thread, time};

use crate::{mq, util, Error, Result};

pub struct Map<Q, R, F>
where
    Q: 'static + Send,
    R: 'static + Send,
    F: 'static + Send + Clone + Fn(Q) -> Result<R>,
{
    name: String,
    chan_size: usize,
    pool_size: Option<usize>,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,

    input: Option<mpsc::Receiver<Q>>,
    map: Option<F>,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl<Q, R, F> Map<Q, R, F>
where
    Q: 'static + Send,
    R: 'static + Send,
    F: 'static + Send + Clone + Fn(Q) -> Result<R>,
{
    pub fn new(name: String, input: mpsc::Receiver<Q>, map: F) -> Self {
        Map {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,
            pool_size: None,
            deadline: None,
            timeout: None,

            input: Some(input),
            map: Some(map),
            handle: None,
        }
    }

    pub fn set_chan_size(&mut self, chan_size: usize) -> &mut Self {
        self.chan_size = chan_size;
        self
    }

    pub fn set_pool_size(&mut self, pool_size: usize) -> &mut Self {
        self.pool_size = Some(pool_size);
        self
    }

    pub fn set_deadline(&mut self, deadline: time::Instant) -> &mut Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn set_timeout(&mut self, timeout: time::Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn spawn(&mut self) -> mpsc::Receiver<R> {
        let (name, chan_size) = (self.name.clone(), self.chan_size);
        let pool_size = self.pool_size;
        let (deadline, timeout) = (self.deadline, self.timeout);
        let (tx, output) = mpsc::sync_channel(self.chan_size);

        let (input, map) = (self.input.take().unwrap(), self.map.take().unwrap());

        self.handle = Some(thread::spawn(move || {
            action(
                name, chan_size, pool_size, deadline, timeout, input, tx, map,
            )
        }));

        output
    }

    pub fn close_wait(self) -> Result<()> {
        match self.handle {
            Some(handle) => match handle.join() {
                Ok(res) => res,
                Err(_) => {
                    err_at!(ThreadFail, msg: "thread fail Map<{:?}>", self.name)
                }
            },
            None => Ok(()),
        }
    }
}

struct Req<Q> {
    seqno: u64,
    qmsg: Q,
}

struct Res<R> {
    seqno: u64,
    rmsg: R,
}

fn action<Q, R, F>(
    name: String,
    chan_size: usize,
    pool_size: Option<usize>,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,
    input: mpsc::Receiver<Q>,
    tx: mpsc::SyncSender<R>,
    map: F,
) -> Result<()>
where
    Q: 'static + Send,
    R: 'static + Send,
    F: 'static + Send + Clone + Fn(Q) -> Result<R>,
{
    let pool = pool_size.map(|pool_size| {
        let mut pool = util::thread::Pool::new_sync(&name, chan_size);
        pool.set_pool_size(pool_size);
        let (map, name) = (map.clone(), name.clone());
        pool.spawn(|rx: util::thread::Rx<Req<Q>, Res<R>>| {
            move || -> Result<()> {
                loop {
                    let (msg, tx) = err_at!(IPCFail, rx.recv())?;
                    let resp = Res {
                        seqno: msg.seqno,
                        rmsg: map(msg.qmsg)?,
                    };
                    err_at!(IPCFail, tx.unwrap().send(resp), "thread Map<{:?}>", name)?;
                }
            }
        });
        pool
    });

    let (mut qseqno, mut rseqno) = (1, 1);
    let mut rmsgs = vec![];
    let (tx_pool, rx_pool) = mpsc::channel();

    let res = 'outer: loop {
        let res = if let Some(deadline) = deadline {
            input.recv_deadline(deadline)
        } else if let Some(timeout) = timeout {
            input.recv_timeout(timeout)
        } else {
            match input.recv() {
                Ok(msg) => Ok(msg),
                Err(_) => Err(mpsc::RecvTimeoutError::Disconnected),
            }
        };

        match (res, &pool) {
            (Ok(msg), Some(pool)) => {
                while let Ok(rmsg) = rx_pool.try_recv() {
                    rmsgs.push(rmsg)
                }
                let res = pool.request_tx(
                    Req {
                        seqno: qseqno,
                        qmsg: msg,
                    },
                    tx_pool.clone(),
                );
                match res {
                    Ok(_) => (),
                    Err(err) => break Err(err),
                }
            }
            (Ok(msg), _) => match map(msg) {
                Ok(rmsg) => rmsgs.push(Res {
                    seqno: qseqno,
                    rmsg,
                }),
                Err(err) => break Err(err),
            },
            (Err(mpsc::RecvTimeoutError::Disconnected), _) => break Ok(()),
            (Err(mpsc::RecvTimeoutError::Timeout), _) => {
                break err_at!(Timeout, msg: "thread Map<{:?}>", name)
            }
        };

        rmsgs.sort_unstable_by_key(|m| m.seqno);
        rmsgs.reverse();
        loop {
            if let Some(rmsg) = rmsgs.pop() {
                if rmsg.seqno == rseqno {
                    rseqno += 1;
                    match tx.send(rmsg.rmsg) {
                        Ok(()) => (),
                        err => {
                            break 'outer err_at!(IPCFail, err, "thread Map<{:?}>", name)
                        }
                    }
                } else {
                    rmsgs.push(rmsg);
                    break;
                }
            }
        }

        qseqno += 1;
    };

    if let Some(pool) = pool {
        pool.close_wait()?;
    }

    // tx shall be dropped here.
    res
}
