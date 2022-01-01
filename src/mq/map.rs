use rayon::prelude::*;

use std::{sync::mpsc, thread};

use crate::{mq, Error, Result};

pub struct Map<Q, R, F>
where
    Q: 'static + Sync + Send,
    R: 'static + Sync + Send,
    F: 'static + Sync + Send + Fn(Q) -> R,
{
    name: String,
    chan_size: usize,

    input: Option<mpsc::Receiver<Q>>,
    map: Option<F>,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl<Q, R, F> Map<Q, R, F>
where
    Q: 'static + Sync + Send,
    R: 'static + Sync + Send,
    F: 'static + Sync + Send + Fn(Q) -> R,
{
    pub fn new(name: String, input: mpsc::Receiver<Q>, map: F) -> Self {
        Map {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,

            input: Some(input),
            map: Some(map),
            handle: None,
        }
    }

    pub fn set_chan_size(&mut self, chan_size: usize) -> &mut Self {
        self.chan_size = chan_size;
        self
    }

    pub fn spawn(&mut self) -> mpsc::Receiver<R> {
        let (name, chan_size) = (self.name.clone(), self.chan_size);
        let (tx, output) = mpsc::sync_channel(self.chan_size);

        let (input, map) = (self.input.take().unwrap(), self.map.take().unwrap());

        self.handle = Some(thread::spawn(move || {
            action(name, chan_size, input, tx, map)
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
    input: mpsc::Receiver<Q>,
    tx: mpsc::SyncSender<R>,
    map: F,
) -> Result<()>
where
    Q: 'static + Sync + Send,
    R: 'static + Sync + Send,
    F: 'static + Sync + Send + Fn(Q) -> R,
{
    loop {
        match mq::get_messages(&input, chan_size) {
            Ok(qmsgs) => {
                for rmsg in qmsgs.into_par_iter().map(&map).collect::<Vec<R>>() {
                    err_at!(IPCFail, tx.send(rmsg), "thread Map<{:?}", name)?
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => break,
            _ => unreachable!(),
        }
    }

    Ok(())
}
