use std::{mem, sync::mpsc, thread, time};

use crate::{Error, Result};

#[derive(Clone)]
pub struct Config {
    pub name: String,
    pub chan_size: usize,
    pub deadline: Option<time::Instant>,
    pub timeout: Option<time::Duration>,
}

pub struct Map<R>
where
    R: 'static + Send,
{
    config: Config,
    handle: thread::JoinHandle<Result<()>>,
    output: Option<mpsc::Receiver<R>>,
}

impl<R> Map<R>
where
    R: 'static + Send,
{
    pub fn new<Q, F>(config: Config, input: mpsc::Receiver<Q>, map: F) -> Map<R>
    where
        Q: 'static + Send,
        F: 'static + Send + FnMut(Q) -> Result<R>,
    {
        let (handle, output) = {
            let config = config.clone();
            let (tx, output) = mpsc::sync_channel(config.chan_size);
            let handle = thread::spawn(move || action(config, input, tx, map));
            (handle, Some(output))
        };

        Map {
            config,
            handle,
            output,
        }
    }

    pub fn output(&mut self) -> mpsc::Receiver<R> {
        self.output.take().unwrap()
    }

    pub fn close_wait(self) -> Result<()> {
        match self.handle.join() {
            Ok(res) => res,
            Err(_) => {
                err_at!(ThreadFail, msg: "thread fail Map<{:?}>", self.config.name)
            }
        }
    }
}

fn action<R, Q, F>(
    config: Config,
    input: mpsc::Receiver<Q>,
    tx: mpsc::SyncSender<R>,
    mut map: F,
) -> Result<()>
where
    Q: 'static + Send,
    R: 'static + Send,
    F: 'static + Send + FnMut(Q) -> Result<R>,
{
    let mut iter = input.iter();
    loop {
        match iter.next() {
            Some(msg) => {
                let resp = map(msg)?;
                err_at!(IPCFail, tx.send(resp), "thread Map<{:?}>", config.name)?;
            }
            None => break,
        }
    }

    mem::drop(tx);

    Ok(())
}
