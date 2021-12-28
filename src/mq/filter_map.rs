use std::{mem, sync::mpsc, thread, time};

use crate::{Error, Result};

#[derive(Clone)]
pub struct Config {
    pub name: String,
    pub chan_size: usize,
    pub deadline: Option<time::Instant>,
    pub timeout: Option<time::Duration>,
}

pub struct FilterMap<R>
where
    R: 'static + Send,
{
    config: Config,
    handle: thread::JoinHandle<Result<()>>,
    output: Option<mpsc::Receiver<R>>,
}

impl<R> FilterMap<R>
where
    R: 'static + Send,
{
    pub fn new<Q, F>(
        config: Config,
        input: mpsc::Receiver<Q>,
        filter_map: F,
    ) -> FilterMap<R>
    where
        Q: 'static + Send,
        F: 'static + Send + FnMut(Q) -> Result<Option<R>>,
    {
        let (handle, output) = {
            let config = config.clone();
            let (tx, output) = mpsc::sync_channel(config.chan_size);
            let handle = thread::spawn(move || action(config, input, tx, filter_map));
            (handle, Some(output))
        };

        FilterMap {
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
                err_at!(ThreadFail, msg: "thread fail FilterMap<{:?}>", self.config.name)
            }
        }
    }
}

fn action<Q, R, F>(
    config: Config,
    input: mpsc::Receiver<Q>,
    tx: mpsc::SyncSender<R>,
    mut filter_map: F,
) -> Result<()>
where
    R: 'static + Send,
    Q: 'static + Send,
    F: 'static + Send + FnMut(Q) -> Result<Option<R>>,
{
    let mut iter = input.iter();
    loop {
        match iter.next() {
            Some(msg) => match filter_map(msg)? {
                Some(resp) => err_at!(
                    IPCFail,
                    tx.send(resp),
                    "thread FilterMap<{:?}>",
                    config.name
                )?,
                None => (),
            },
            None => break,
        }
    }

    mem::drop(tx);

    Ok(())
}
