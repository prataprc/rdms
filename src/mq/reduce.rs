use rayon::prelude::*;

use std::{sync::mpsc, thread};

use crate::{mq, Error, Result};

pub struct Reduce<Q, ID, F>
where
    Q: 'static + Sync + Send,
    ID: 'static + Sync + Send + Clone + Fn() -> Q,
    F: 'static + Sync + Send + Fn(Q, Q) -> Q,
{
    name: String,
    chan_size: usize,

    input: Option<mpsc::Receiver<Q>>,
    identity: Option<ID>,
    reduce: Option<F>,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl<Q, ID, F> Reduce<Q, ID, F>
where
    Q: 'static + Sync + Send,
    ID: 'static + Sync + Send + Clone + Fn() -> Q,
    F: 'static + Sync + Send + Fn(Q, Q) -> Q,
{
    pub fn new(name: String, input: mpsc::Receiver<Q>, identity: ID, reduce: F) -> Self {
        Reduce {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,

            input: Some(input),
            identity: Some(identity),
            reduce: Some(reduce),
            handle: None,
        }
    }

    pub fn set_chan_size(&mut self, chan_size: usize) -> &mut Self {
        self.chan_size = chan_size;
        self
    }

    pub fn spawn(&mut self) -> mpsc::Receiver<Q> {
        let (name, chan_size) = (self.name.clone(), self.chan_size);
        let (tx, output) = mpsc::sync_channel(self.chan_size);

        let input = self.input.take().unwrap();
        let identity = self.identity.take().unwrap();
        let reduce = self.reduce.take().unwrap();

        self.handle = Some(thread::spawn(move || {
            action(name, chan_size, input, tx, identity, reduce)
        }));

        output
    }

    pub fn close_wait(self) -> Result<()> {
        match self.handle {
            Some(handle) => match handle.join() {
                Ok(res) => res,
                Err(_) => {
                    err_at!(ThreadFail, msg: "thread fail Reduce<{:?}>", self.name)
                }
            },
            None => Ok(()),
        }
    }
}

fn action<Q, ID, F>(
    name: String,
    chan_size: usize,
    input: mpsc::Receiver<Q>,
    tx: mpsc::SyncSender<Q>,
    identity: ID,
    reduce: F,
) -> Result<()>
where
    Q: 'static + Sync + Send,
    ID: 'static + Sync + Send + Clone + Fn() -> Q,
    F: 'static + Sync + Send + Fn(Q, Q) -> Q,
{
    let mut qmsg = None;
    loop {
        match mq::get_messages(&input, chan_size) {
            Ok(mut qmsgs) => {
                qmsg.map(|qmsg| qmsgs.insert(0, qmsg));
                qmsg = Some(
                    qmsgs
                        .into_par_iter()
                        .reduce(identity.clone(), |a, b| reduce(a, b)),
                );
            }
            Err(mpsc::TryRecvError::Disconnected) => break,
            _ => unreachable!(),
        }
    }

    match qmsg {
        Some(rmsg) => err_at!(IPCFail, tx.send(rmsg), "thread Reduce<{:?}", name),
        None => Ok(()),
    }
}
