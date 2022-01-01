use rayon::prelude::*;

use std::{sync::mpsc, thread};

use crate::{mq, Error, Result};

pub struct FilterMap<Q, R, F>
where
    Q: 'static + Sync + Send,
    R: 'static + Sync + Send,
    F: 'static + Sync + Send + Fn(Q) -> Option<R>,
{
    name: String,
    chan_size: usize,

    input: Option<mpsc::Receiver<Q>>,
    filter_map: Option<F>,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl<Q, R, F> FilterMap<Q, R, F>
where
    Q: 'static + Sync + Send,
    R: 'static + Sync + Send,
    F: 'static + Sync + Send + Fn(Q) -> Option<R>,
{
    pub fn new(name: String, input: mpsc::Receiver<Q>, filter_map: F) -> Self {
        FilterMap {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,

            input: Some(input),
            filter_map: Some(filter_map),
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

        let input = self.input.take().unwrap();
        let filter_map = self.filter_map.take().unwrap();

        self.handle = Some(thread::spawn(move || {
            action(name, chan_size, input, tx, filter_map)
        }));

        output
    }

    pub fn close_wait(self) -> Result<()> {
        match self.handle {
            Some(handle) => match handle.join() {
                Ok(res) => res,
                Err(_) => {
                    err_at!(ThreadFail, msg: "thread fail FilterMap<{:?}>", self.name)
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
    filter_map: F,
) -> Result<()>
where
    R: 'static + Sync + Send,
    Q: 'static + Sync + Send,
    F: 'static + Sync + Send + Fn(Q) -> Option<R>,
{
    loop {
        match mq::get_messages(&input, chan_size) {
            Ok(qmsgs) => {
                for rmsg in qmsgs
                    .into_par_iter()
                    .filter_map(&filter_map)
                    .collect::<Vec<R>>()
                {
                    err_at!(IPCFail, tx.send(rmsg), "thread FilterMap<{:?}", name)?
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => break,
            _ => unreachable!(),
        }
    }

    Ok(())
}
