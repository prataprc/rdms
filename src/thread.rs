//! Module `thread` implement a generic multi-threading pattern for [rdms]
//! components.
//!
//! It is inspired from gen-server model from Erlang, where by every thread
//! is expected hold onto its own state, FnOnce in rust parlance, and handle
//! all inter-thread communication via channels and message queues.

use log::{error, info};

#[allow(unused_imports)]
use std::{
    mem,
    sync::{mpsc, Arc},
    thread,
};

#[allow(unused_imports)]
use crate::rdms;
use crate::{core::Result, error::Error, robt::Flusher};

/// IPC type, that enumerates as either [std::sync::mpsc::Sender] or,
/// [std::sync::mpsc::SyncSender].
///
/// The clone behavior is similar to [std::sync::mpsc::Sender] or,
/// [std::sync::mpsc::Sender].
enum Tx<Q, R> {
    N(mpsc::Sender<(Q, Option<mpsc::Sender<R>>)>),
    S(mpsc::SyncSender<(Q, Option<mpsc::Sender<R>>)>),
}

impl<Q, R> Clone for Tx<Q, R> {
    fn clone(&self) -> Self {
        match self {
            Tx::N(tx) => Tx::N(tx.clone()),
            Tx::S(tx) => Tx::S(tx.clone()),
        }
    }
}

/// IPC type, that shall be passed to the thread's main loop.
///
/// Refer to [Thread::new] for details.
pub type Rx<Q, R> = mpsc::Receiver<(Q, Option<mpsc::Sender<R>>)>;

/// Thread type, providing gen-server pattern to do multi-threading.
///
/// When a thread value is dropped, it is made sure that there are
/// no dangling thread routines. To acheive this following requirements
/// need to be satisfied:
///
/// * The thread's main loop should handle _disconnect_ signal on its
///   [Rx] channel.
/// * All [Client] handles on this thread should be dropped as well.
pub struct Thread<Q, R, T> {
    name: String,
    inner: Option<Inner<Q, R, T>>,
    refn: Arc<bool>,
}

struct Inner<Q, R, T> {
    tx: Tx<Q, R>,
    handle: thread::JoinHandle<Result<T>>,
}

impl<Q, R, T> Inner<Q, R, T> {
    fn close_wait(self) -> Result<T> {
        mem::drop(self.tx); // drop input channel to thread.

        match self.handle.join().unwrap() {
            Ok(exit) => Ok(exit),
            Err(err) => Err(err),
        }
    }
}

impl<Q, R, T> Drop for Thread<Q, R, T> {
    fn drop(&mut self) {
        let _ = loop {
            match Arc::get_mut(&mut self.refn) {
                Some(_) => match self.inner.take() {
                    Some(inner) => break inner.close_wait().ok(),
                    None => error!(target: "thread", "{}:{} unreachable",  file!(), line!()),
                },
                None => error!(target: "thread", "active clients"),
            }
        };

        info!(target: "thread", "{} dropped", self.name);
    }
}

impl<Q, R, T> Thread<Q, R, T> {
    /// Create a new Thread instance, using asynchronous channel with
    /// infinite buffer.
    pub fn new<F, N>(name: String, main_loop: F) -> Thread<Q, R, T>
    where
        F: 'static + FnOnce(Rx<Q, R>) -> N + Send,
        N: 'static + Send + FnOnce() -> Result<T>,
        T: 'static + Send,
    {
        info!(target: "thread", "{} spawned in async mode", name);

        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(main_loop(rx));
        Thread {
            name,
            inner: Some(Inner {
                tx: Tx::N(tx),
                handle,
            }),
            refn: Arc::new(true),
        }
    }

    /// Create a new Thread instance, using synchronous channel with
    /// finite buffer.
    pub fn new_sync<F, N>(name: String, main_loop: F, channel_size: usize) -> Thread<Q, R, T>
    where
        F: 'static + FnOnce(Rx<Q, R>) -> N + Send,
        N: 'static + Send + FnOnce() -> Result<T>,
        T: 'static + Send,
    {
        info!(target: "thread", "{} spawned in sync mode", name);

        let (tx, rx) = mpsc::sync_channel(channel_size);
        let handle = thread::spawn(main_loop(rx));
        Thread {
            name,
            inner: Some(Inner {
                tx: Tx::S(tx),
                handle,
            }),
            refn: Arc::new(true),
        }
    }

    /// Create a new write handle to communicate with this thread.
    ///
    /// NOTE: All write handles must be dropped for the thread to exit.
    pub fn to_client(&self) -> Client<Q, R> {
        let _refn = Arc::clone(&self.refn);
        Client {
            tx: self.inner.as_ref().unwrap().tx.clone(),
            _refn,
        }
    }

    /// Post a message to thread and don't wait for response.
    pub fn post(&self, msg: Q) -> Result<()> {
        match &self.inner {
            Some(inner) => {
                match &inner.tx {
                    Tx::N(thread_tx) => err_at!(IPCFail, thread_tx.send((msg, None)))?,
                    Tx::S(thread_tx) => err_at!(IPCFail, thread_tx.send((msg, None)))?,
                };
                Ok(())
            }
            None => err_at!(Fatal, msg: format!("Thread.pos()")),
        }
    }

    /// Send a request message to thread and wait for a response.
    pub fn request(&self, request: Q) -> Result<R> {
        match &self.inner {
            Some(inner) => {
                let (tx, rx) = mpsc::channel();
                match &inner.tx {
                    Tx::N(thread_tx) => {
                        //
                        err_at!(IPCFail, thread_tx.send((request, Some(tx))))?
                    }
                    Tx::S(thread_tx) => {
                        //
                        err_at!(IPCFail, thread_tx.send((request, Some(tx))))?
                    }
                }
                Ok(err_at!(IPCFail, rx.recv())?)
            }
            None => err_at!(Fatal, msg: format!("Thread.request()")),
        }
    }

    /// Return ref_count on this thread. This matches number of [Client]
    /// handle + 1.
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.refn)
    }

    /// Recommended way to exit/shutdown the thread.
    ///
    /// Even otherwise, when Thread value goes out of scope its drop
    /// implementation shall call this method to exit the thread, except
    /// that any errors are ignored.
    pub fn close_wait(mut self) -> Result<T> {
        match self.inner.take() {
            Some(inner) => inner.close_wait(),
            None => err_at!(Fatal, msg: format!("Thread.close_wait()")),
        }
    }
}

impl<R, T> Flusher for Thread<Vec<u8>, R, T> {
    #[inline]
    fn post(&self, msg: Vec<u8>) -> Result<()> {
        self.post(msg)
    }
}

/// Client handle to communicate with thread. Applications can create as many
/// Client handles as needed.
pub struct Client<Q, R> {
    tx: Tx<Q, R>,
    _refn: Arc<bool>,
}

impl<Q, R> Client<Q, R> {
    /// Same as [Thread::post] method.
    pub fn post(&mut self, msg: Q) -> Result<()> {
        match &self.tx {
            Tx::N(thread_tx) => Ok(err_at!(IPCFail, thread_tx.send((msg, None)))?),
            Tx::S(thread_tx) => Ok(err_at!(IPCFail, thread_tx.send((msg, None)))?),
        }
    }

    /// Same as [Thread::request] method.
    pub fn request(&mut self, request: Q) -> Result<R> {
        let (tx, rx) = mpsc::channel();
        match &self.tx {
            Tx::N(thread_tx) => {
                //
                err_at!(IPCFail, thread_tx.send((request, Some(tx))))?
            }
            Tx::S(thread_tx) => {
                //
                err_at!(IPCFail, thread_tx.send((request, Some(tx))))?
            }
        }
        Ok(err_at!(IPCFail, rx.recv())?)
    }
}
