//! Module `thread` implement a generic multi-threading pattern.
//!
//! It is inspired from gen-server model from Erlang, where by, every thread is
//! expected to hold onto its own state, and handle all inter-thread communication
//! via channels and message queues.

use std::{marker::PhantomData, sync::mpsc, thread};

use crate::{Error, Result};

/// IPC type, that enumerates as either [mpsc::Sender] or, [mpsc::SyncSender]
/// channel.
///
/// The clone behavior is similar to [mpsc::Sender] or, [mpsc::SyncSender].
pub enum Tx<Q, R = ()> {
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

impl<Q, R> Tx<Q, R> {
    /// Post a message to thread and don't wait for response.
    pub fn post(&self, msg: Q) -> Result<()> {
        match self {
            Tx::N(tx) => err_at!(IPCFail, tx.send((msg, None)))?,
            Tx::S(tx) => err_at!(IPCFail, tx.send((msg, None)))?,
        };
        Ok(())
    }

    /// Send a request message to thread and wait for a response.
    pub fn request(&self, request: Q) -> Result<R> {
        let (stx, srx) = mpsc::channel();
        match self {
            Tx::N(tx) => err_at!(IPCFail, tx.send((request, Some(stx))))?,
            Tx::S(tx) => err_at!(IPCFail, tx.send((request, Some(stx))))?,
        }
        err_at!(IPCFail, srx.recv())
    }
}

/// IPC type, that shall be passed to the thread's main loop.
///
/// Refer to [Thread::new] for details.
pub type Rx<Q, R = ()> = mpsc::Receiver<(Q, Option<mpsc::Sender<R>>)>;

/// Thread type, providing gen-server pattern to do multi-threading.
///
/// When a thread value is dropped, it is made sure that there are
/// no dangling thread routines. To achieve this following requirements
/// need to be satisfied:
///
/// * The thread's main loop should handle _disconnect_ signal on its
///   [Rx] channel.
/// * Call `close_wait()` on the [Thread] instance.
pub struct Thread<Q, R = (), T = ()> {
    name: String,
    inner: Option<Inner<Q, R, T>>,
}

struct Inner<Q, R, T> {
    handle: thread::JoinHandle<T>,
    _req: PhantomData<Q>,
    _res: PhantomData<R>,
}

impl<Q, R, T> Inner<Q, R, T> {
    fn join(self) -> Result<T> {
        match self.handle.join() {
            Ok(val) => Ok(val),
            Err(err) => err_at!(ThreadFail, msg: "fail {:?}", err),
        }
    }
}

impl<Q, R, T> Drop for Thread<Q, R, T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.join().ok();
        }
    }
}

impl<Q, R, T> Thread<Q, R, T> {
    /// Create a new Thread instance, using asynchronous channel with
    /// infinite buffer. `main_loop` shall be called with the rx side
    /// of the channel and shall return a function that can be spawned
    /// using thread::spawn.
    pub fn new<F, N>(name: &str, main_loop: F) -> (Thread<Q, R, T>, Tx<Q, R>)
    where
        F: 'static + FnOnce(Rx<Q, R>) -> N + Send,
        N: 'static + Send + FnOnce() -> T,
        T: 'static + Send,
    {
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(main_loop(rx));

        let th = Thread {
            name: name.to_string(),
            inner: Some(Inner {
                handle,
                _req: PhantomData,
                _res: PhantomData,
            }),
        };

        (th, Tx::N(tx))
    }

    /// Create a new Thread instance, using synchronous channel with
    /// finite buffer.
    pub fn new_sync<F, N>(
        name: &str,
        channel_size: usize,
        main_loop: F,
    ) -> (Thread<Q, R, T>, Tx<Q, R>)
    where
        F: 'static + FnOnce(Rx<Q, R>) -> N + Send,
        N: 'static + Send + FnOnce() -> T,
        T: 'static + Send,
    {
        let (tx, rx) = mpsc::sync_channel(channel_size);
        let handle = thread::spawn(main_loop(rx));

        let th = Thread {
            name: name.to_string(),
            inner: Some(Inner {
                handle,
                _req: PhantomData,
                _res: PhantomData,
            }),
        };

        (th, Tx::S(tx))
    }

    /// Recommended way to exit/shutdown the thread. Note that all [Tx]
    /// clones of this thread must also be dropped for this call to return.
    ///
    /// Even otherwise, when Thread value goes out of scope its drop
    /// implementation shall call this method to exit the thread, except
    /// that any errors are ignored.
    pub fn join(mut self) -> Result<T> {
        self.inner.take().unwrap().join()
    }
}
