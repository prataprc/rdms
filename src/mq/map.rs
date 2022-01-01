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
    let pool = pool_size
        .map(|pool_size| make_thread_pool(&name, pool_size, chan_size, map.clone()));

    let res = action_loop(&name, deadline, timeout, pool.as_ref(), input, tx, map);

    if let Some(pool) = pool {
        pool.close_wait()?;
    }

    // tx shall be dropped here.
    res
}

fn action_loop<Q, R, F>(
    name: &str,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,
    pool: Option<&util::thread::Pool<mq::Req<Q>, mq::Res<R>, Result<()>>>,
    input: mpsc::Receiver<Q>,
    tx: mpsc::SyncSender<R>,
    map: F,
) -> Result<()>
where
    Q: 'static + Send,
    R: 'static + Send,
    F: 'static + Send + Clone + Fn(Q) -> Result<R>,
{
    let (tx_pool, rx_pool) = mpsc::channel();
    let mut rmsgs = vec![];

    let (mut qseqno, mut rseqno) = (1, 1);

    loop {
        let res = mq::get_message(&input, deadline, timeout);

        match (res, &pool) {
            (Ok(qmsg), None) => rmsgs.push(mq::Res::new(qseqno, map(qmsg)?)),
            (Ok(qmsg), Some(pool)) => {
                // first drain out response-messages (processed by the pool)
                while let Ok(rmsg) = rx_pool.try_recv() {
                    rmsgs.push(rmsg)
                }
                // then request message-processing from thread-pool
                pool.request_tx(mq::Req::<Q>::new(qseqno, qmsg), tx_pool.clone())?;
            }
            (Err(mpsc::RecvTimeoutError::Disconnected), _) => break Ok(()),
            (Err(mpsc::RecvTimeoutError::Timeout), _) => {
                break err_at!(Timeout, msg: "thread Map<{:?}>", name)
            }
        };

        rseqno = err_at!(IPCFail, mq::put_messages(&mut rmsgs, rseqno, &tx))?;
        qseqno += 1;
    }
}

fn make_thread_pool<Q, R, F>(
    name: &str,
    pool_size: usize,
    chan_size: usize,
    map: F,
) -> util::thread::Pool<mq::Req<Q>, mq::Res<R>, Result<()>>
where
    Q: 'static + Send,
    R: 'static + Send,
    F: 'static + Send + Clone + Fn(Q) -> Result<R>,
{
    let mut pool = util::thread::Pool::new_sync(name, chan_size);
    pool.set_pool_size(pool_size);

    let (map, name) = (map.clone(), name.to_string());

    pool.spawn(|rx: util::thread::Rx<mq::Req<Q>, mq::Res<R>>| {
        move || -> Result<()> {
            loop {
                let (req, tx) = err_at!(IPCFail, rx.recv())?;
                let resp = mq::Res {
                    seqno: req.seqno,
                    rmsg: map(req.qmsg)?,
                };
                err_at!(IPCFail, tx.unwrap().send(resp), "thread Map<{:?}>", name)?;
            }
        }
    });

    pool
}
