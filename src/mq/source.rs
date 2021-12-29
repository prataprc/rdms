use std::{sync::mpsc, thread};

use crate::{mq, Error, Result};

/// Source type, than can create messages either from iterator or generator function.
pub struct Source<R, I, F>
where
    R: 'static + Send,
    I: 'static + Send + Iterator<Item = R>,
    F: 'static + Send + FnMut() -> Result<Option<R>>,
{
    name: String,
    chan_size: usize,

    inner: Option<Inner<R, I, F>>,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

enum Inner<R, I, F>
where
    R: 'static + Send,
    I: 'static + Send + Iterator<Item = R>,
    F: 'static + Send + FnMut() -> Result<Option<R>>,
{
    Iter { iter: I },
    Gen { gen: F },
}

impl<R, I, F> Source<R, I, F>
where
    R: 'static + Send,
    I: 'static + Send + Iterator<Item = R>,
    F: 'static + Send + FnMut() -> Result<Option<R>>,
{
    /// Create a new source from iterator.
    pub fn from_iter(name: String, iter: I) -> Self {
        Source {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,

            inner: Some(Inner::Iter { iter }),
            handle: None,
        }
    }

    /// Create a new source from generator function.
    pub fn from_gen(name: String, gen: F) -> Self {
        Source {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,

            inner: Some(Inner::Gen { gen }),
            handle: None,
        }
    }

    pub fn set_chan_size(&mut self, chan_size: usize) -> &mut Self {
        self.chan_size = chan_size;
        self
    }

    /// Take the output channel for this source. Shall be called only once after
    /// creating the source.
    pub fn spawn(&mut self) -> mpsc::Receiver<R> {
        let name = self.name.clone();

        let (handle, output) = match self.inner.take() {
            Some(Inner::Iter { iter }) => {
                let (tx, output) = mpsc::sync_channel(self.chan_size);
                (thread::spawn(move || action_iter(name, tx, iter)), output)
            }
            Some(Inner::Gen { gen }) => {
                let (tx, output) = mpsc::sync_channel(self.chan_size);
                (thread::spawn(move || action_gen(name, tx, gen)), output)
            }
            None => unreachable!(),
        };

        self.handle = Some(handle);
        output
    }

    /// Close this source.
    pub fn close_wait(self) -> Result<()> {
        match self.handle {
            Some(handle) => match handle.join() {
                Ok(res) => res,
                Err(_) => {
                    err_at!(ThreadFail, msg: "thread fail Source<{:?}>", self.name)
                }
            },
            None => Ok(()),
        }
    }
}

fn action_iter<R, I>(name: String, tx: mpsc::SyncSender<R>, mut iter: I) -> Result<()>
where
    R: 'static + Send,
    I: 'static + Send + Iterator<Item = R>,
{
    loop {
        match iter.next() {
            Some(msg) => err_at!(IPCFail, tx.send(msg), "thread Source<{:?}>", name)?,
            None => break,
        }
    }

    // tx shall be dropped here.
    Ok(())
}

fn action_gen<R, F>(name: String, tx: mpsc::SyncSender<R>, mut gen: F) -> Result<()>
where
    R: 'static + Send,
    F: 'static + Send + FnMut() -> Result<Option<R>>,
{
    loop {
        match gen()? {
            Some(msg) => err_at!(IPCFail, tx.send(msg), "thread Source<{:?}>", name)?,
            None => break,
        }
    }

    // tx shall be dropped here.
    Ok(())
}
