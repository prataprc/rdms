use std::{mem, sync::mpsc, thread, time};

use crate::{Error, Result};

#[derive(Clone)]
pub struct Config {
    pub name: String,
    pub chan_size: usize,
    pub deadline: Option<time::Instant>,
    pub timeout: Option<time::Duration>,
}

pub struct Reduce<R>
where
    R: 'static + Send,
{
    config: Config,
    handle: thread::JoinHandle<Result<()>>,
    output: Option<mpsc::Receiver<R>>,
}

impl<R> Reduce<R>
where
    R: 'static + Send,
{
    pub fn new<Q, F>(
        config: Config,
        input: mpsc::Receiver<Q>,
        initial: R,
        reduce: F,
    ) -> Reduce<R>
    where
        Q: 'static + Send,
        F: 'static + Send + FnMut(R, Q) -> Result<R>,
    {
        let (handle, output) = {
            let config = config.clone();
            let (tx, output) = mpsc::sync_channel(config.chan_size);
            let handle =
                thread::spawn(move || action(config, input, initial, tx, reduce));
            (handle, Some(output))
        };

        Reduce {
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
                err_at!(ThreadFail, msg: "thread fail Reduce<{:?}>", self.config.name)
            }
        }
    }
}

fn action<Q, R, F>(
    config: Config,
    input: mpsc::Receiver<Q>,
    initial: R,
    tx: mpsc::SyncSender<R>,
    mut reduce: F,
) -> Result<()>
where
    R: 'static + Send,
    Q: 'static + Send,
    F: 'static + Send + FnMut(R, Q) -> Result<R>,
{
    let mut iter = input.iter();
    let mut value = initial;
    loop {
        match iter.next() {
            Some(msg) => {
                value = reduce(value, msg)?;
            }
            None => break,
        }
    }

    err_at!(IPCFail, tx.send(value), "thread Reduce<{:?}>", config.name)?;

    mem::drop(tx);

    Ok(())
}
