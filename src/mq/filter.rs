use rayon::prelude::*;

use std::{sync::mpsc, thread};

use crate::{mq, Error, Result};

pub struct Filter<Q, F>
where
    Q: 'static + Sync + Send,
    F: 'static + Sync + Send + Fn(&Q) -> bool,
{
    name: String,
    chan_size: usize,

    input: Option<mpsc::Receiver<Q>>,
    filter: Option<F>,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl<Q, F> Filter<Q, F>
where
    Q: 'static + Sync + Send,
    F: 'static + Sync + Send + Fn(&Q) -> bool,
{
    pub fn new(name: String, input: mpsc::Receiver<Q>, filter: F) -> Self {
        Filter {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,

            input: Some(input),
            filter: Some(filter),
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

        let (input, filter) = (self.input.take().unwrap(), self.filter.take().unwrap());

        self.handle = Some(thread::spawn(move || {
            action(name, chan_size, input, tx, filter)
        }));

        output
    }

    pub fn close_wait(self) -> Result<()> {
        match self.handle {
            Some(handle) => match handle.join() {
                Ok(res) => res,
                Err(_) => {
                    err_at!(ThreadFail, msg: "thread fail Filter<{:?}>", self.name)
                }
            },
            None => Ok(()),
        }
    }
}

fn action<Q, F>(
    name: String,
    chan_size: usize,
    input: mpsc::Receiver<Q>,
    tx: mpsc::SyncSender<Q>,
    filter: F,
) -> Result<()>
where
    Q: 'static + Sync + Send,
    F: 'static + Sync + Send + Fn(&Q) -> bool,
{
    loop {
        match mq::get_messages(&input, chan_size) {
            Ok(qmsgs) => {
                for rmsg in qmsgs.into_par_iter().filter(&filter).collect::<Vec<Q>>() {
                    err_at!(IPCFail, tx.send(rmsg), "thread Filter<{:?}", name)?
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => break,
            _ => unreachable!(),
        }
    }

    Ok(())
}
