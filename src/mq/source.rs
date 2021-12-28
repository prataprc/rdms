use std::{mem, sync::mpsc, thread};

use crate::{Error, Result};

/// Configuration for [Source] type, shall be used to create a new source.
#[derive(Clone)]
pub struct Config {
    pub name: String,
    pub chan_size: usize,
}

/// Source type, than can create messages either from iterator or generator function.
pub struct Source<R>
where
    R: 'static + Send,
{
    config: Config,
    handle: thread::JoinHandle<Result<()>>,
    output: Option<mpsc::Receiver<R>>,
}

impl<R> Source<R>
where
    R: 'static + Send,
{
    /// Create a new source from iterator.
    pub fn from_iter<I>(config: Config, iter: I) -> Source<R>
    where
        I: 'static + Send + Iterator<Item = R>,
    {
        let (handle, output) = {
            let config = config.clone();
            let (tx, output) = mpsc::sync_channel(config.chan_size);
            let handle = thread::spawn(move || action_iter(config, tx, iter));
            (handle, Some(output))
        };

        Source {
            config,
            handle,
            output,
        }
    }

    /// Create a new source from generator function.
    pub fn from_gen<F>(config: Config, gen: F) -> Source<R>
    where
        F: 'static + Send + FnMut() -> Result<Option<R>>,
    {
        let (handle, output) = {
            let config = config.clone();
            let (tx, output) = mpsc::sync_channel(config.chan_size);
            let handle = thread::spawn(move || action_gen(config, tx, gen));
            (handle, Some(output))
        };

        Source {
            config,
            handle,
            output,
        }
    }

    /// Take the output channel for this source. Shall be called only once after
    /// creating the source.
    pub fn output(&mut self) -> mpsc::Receiver<R> {
        self.output.take().unwrap()
    }

    /// Close this source.
    pub fn close_wait(self) -> Result<()> {
        match self.handle.join() {
            Ok(res) => res,
            Err(_) => {
                err_at!(ThreadFail, msg: "thread fail Source<{:?}>", self.config.name)
            }
        }
    }
}

fn action_iter<R, I>(config: Config, tx: mpsc::SyncSender<R>, mut iter: I) -> Result<()>
where
    R: 'static + Send,
    I: 'static + Send + Iterator<Item = R>,
{
    loop {
        match iter.next() {
            Some(msg) => {
                err_at!(IPCFail, tx.send(msg), "thread Source<{:?}>", config.name)?
            }
            None => break,
        }
    }

    mem::drop(tx);

    Ok(())
}

fn action_gen<R, F>(config: Config, tx: mpsc::SyncSender<R>, mut gen: F) -> Result<()>
where
    R: 'static + Send,
    F: 'static + Send + FnMut() -> Result<Option<R>>,
{
    loop {
        match gen()? {
            Some(msg) => {
                err_at!(IPCFail, tx.send(msg), "thread Source<{:?}>", config.name)?
            }
            None => break,
        }
    }

    mem::drop(tx);

    Ok(())
}
