use std::{mem, sync::mpsc, thread};

use crate::{core::Result, error::Error, robt::Flusher};

enum Tx<Q, R> {
    N(mpsc::Sender<(Q, Option<mpsc::Sender<R>>)>),
    S(mpsc::SyncSender<(Q, Option<mpsc::Sender<R>>)>),
}

pub struct Thread<Q, R, T> {
    inner: Option<Inner<Q, R, T>>,
}

impl<Q, R, T> Drop for Thread<Q, R, T> {
    fn drop(&mut self) {
        match self.inner.take() {
            Some(inner) => inner.close_wait().ok(),
            None => None,
        };
    }
}

impl<Q, R, T> Thread<Q, R, T> {
    pub fn new<F, N>(main_loop: F) -> Thread<Q, R, T>
    where
        F: 'static + FnOnce(mpsc::Receiver<(Q, Option<mpsc::Sender<R>>)>) -> N + Send,
        N: 'static + Send + FnOnce() -> Result<T>,
        T: 'static + Send,
    {
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(main_loop(rx));
        Thread {
            inner: Some(Inner {
                tx: Tx::N(tx),
                handle,
            }),
        }
    }

    pub fn new_sync<F, N>(main_loop: F, channel_size: usize) -> Thread<Q, R, T>
    where
        F: 'static + FnOnce(mpsc::Receiver<(Q, Option<mpsc::Sender<R>>)>) -> N + Send,
        N: 'static + Send + FnOnce() -> Result<T>,
        T: 'static + Send,
    {
        let (tx, rx) = mpsc::sync_channel(channel_size);
        let handle = thread::spawn(main_loop(rx));
        Thread {
            inner: Some(Inner {
                tx: Tx::S(tx),
                handle,
            }),
        }
    }

    pub fn post(&self, msg: Q) -> Result<()> {
        match &self.inner {
            Some(inner) => {
                match &inner.tx {
                    Tx::N(thread_tx) => thread_tx.send((msg, None))?,
                    Tx::S(thread_tx) => thread_tx.send((msg, None))?,
                };
                Ok(())
            }
            None => Err(Error::UnInitialized(format!("Thread not initialized"))),
        }
    }
    pub fn request(&self, request: Q) -> Result<R> {
        match &self.inner {
            Some(inner) => {
                let (tx, rx) = mpsc::channel();
                match &inner.tx {
                    Tx::N(thread_tx) => thread_tx.send((request, Some(tx)))?,
                    Tx::S(thread_tx) => thread_tx.send((request, Some(tx)))?,
                }
                Ok(rx.recv()?)
            }
            None => Err(Error::UnInitialized(format!("Thread not initialized"))),
        }
    }

    pub fn close_wait(mut self) -> Result<T> {
        match self.inner.take() {
            Some(inner) => inner.close_wait(),
            None => Err(Error::UnInitialized(format!("Thread not initialized"))),
        }
    }
}

impl<R, T> Flusher for Thread<Vec<u8>, R, T> {
    #[inline]
    fn post(&self, msg: Vec<u8>) -> Result<()> {
        self.post(msg)
    }
}

struct Inner<Q, R, T> {
    tx: Tx<Q, R>,
    handle: thread::JoinHandle<Result<T>>,
}

impl<Q, R, T> Inner<Q, R, T> {
    pub fn close_wait(self) -> Result<T> {
        mem::drop(self.tx); // drop input channel to thread.

        match self.handle.join() {
            Ok(Ok(exit)) => Ok(exit),
            Ok(Err(err)) => Err(err),
            Err(err) => {
                let err = Error::ThreadFail(format!("{:?}", err));
                Err(err)
            }
        }
    }
}
