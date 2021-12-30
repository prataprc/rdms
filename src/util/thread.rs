//! Module `thread` implement a generic multi-threading pattern.
//!
//! It is inspired from gen-server model from Erlang, where by, every thread is
//! expected to hold onto its own state, and handle all inter-thread communication
//! via channels and message queues.

use std::{
    mem,
    sync::{mpsc, Arc, Mutex},
    thread,
};

use crate::{Error, Result};

/// Thread type, providing gen-server pattern to do multi-threading.
///
/// NOTE: When a thread value is dropped, it is made sure that there are no dangling
/// thread routines. To achieve this following requirements need to be satisfied:
///
/// * The thread's main loop should handle _disconnect_ signal on its [Rx] channel.
/// * Call `join()` on the [Thread] instance.
pub struct Thread<Q, R = (), T = ()> {
    name: String,
    inner: Option<Inner<Q, R, T>>,
}

struct Inner<Q, R, T> {
    handle: thread::JoinHandle<T>,
    tx: Option<Arc<Mutex<Tx<Q, R>>>>,
}

impl<Q, R, T> Inner<Q, R, T> {
    fn join(mut self) -> Result<T> {
        mem::drop(self.tx.take());

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
    /// Create a new Thread instance, using asynchronous channel with infinite buffer.
    /// `main_loop` shall be called with the rx side of the channel and shall return
    /// a function that can be spawned using thread::spawn.
    pub fn new<F, N>(name: &str, main_loop: F) -> Thread<Q, R, T>
    where
        F: 'static + FnOnce(Rx<Q, R>) -> N + Send,
        N: 'static + Send + FnOnce() -> T,
        T: 'static + Send,
    {
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(main_loop(rx));

        let tx = Some(Arc::new(Mutex::new(Tx::N(tx))));

        Thread {
            name: name.to_string(),
            inner: Some(Inner { handle, tx }),
        }
    }

    /// Create a new Thread instance, using synchronous channel with finite buffer.
    pub fn new_sync<F, N>(name: &str, chan_size: usize, main_loop: F) -> Thread<Q, R, T>
    where
        F: 'static + FnOnce(Rx<Q, R>) -> N + Send,
        N: 'static + Send + FnOnce() -> T,
        T: 'static + Send,
    {
        let (tx, rx) = mpsc::sync_channel(chan_size);
        let handle = thread::spawn(main_loop(rx));

        let tx = Some(Arc::new(Mutex::new(Tx::S(tx))));

        Thread {
            name: name.to_string(),
            inner: Some(Inner { handle, tx }),
        }
    }

    /// Recommended way to exit/shutdown the thread. Note that all [Tx] clones of this
    /// thread must also be dropped for this call to return.
    ///
    /// Even otherwise, when Thread value goes out of scope its drop implementation
    /// shall call this method to exit the thread, except that any errors are ignored.
    pub fn join(mut self) -> Result<T> {
        self.inner.take().unwrap().join()
    }

    /// Return name of this thread.
    pub fn to_name(&self) -> String {
        self.name.to_string()
    }

    /// Return a clone of tx channel.
    pub fn to_tx(&self) -> Tx<Q, R> {
        match self.inner.as_ref() {
            Some(inner) => inner.tx.as_ref().unwrap().lock().unwrap().clone(),
            None => unreachable!(),
        }
    }
}

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

    /// Send a request message to thread and wait for a response.
    pub fn request_tx(&self, request: Q, rt_tx: mpsc::Sender<R>) -> Result<()> {
        match self {
            Tx::N(tx) => err_at!(IPCFail, tx.send((request, Some(rt_tx)))),
            Tx::S(tx) => err_at!(IPCFail, tx.send((request, Some(rt_tx)))),
        }
    }
}

/// IPC type, that shall be passed to the thread's main loop.
///
/// Refer to [Thread::new] for details.
pub type Rx<Q, R = ()> = mpsc::Receiver<(Q, Option<mpsc::Sender<R>>)>;

/// Create a pool of threads of same type.
///
/// That is, the thread's main-loop takes the same Request type and return the same
/// Response type. Load balancing across the threads are handled in random fashion.
pub struct Pool<Q, R = (), T = ()> {
    name: String,
    threads: Vec<Thread<Q, R, T>>,
    pool_size: usize,
    chan_size: Option<usize>,
}

impl<Q, R, T> Pool<Q, R, T> {
    /// Create a new pool, number of threads in this pool shall default to number of cores.
    pub fn new(name: &str) -> Pool<Q, R, T> {
        Pool {
            name: name.to_string(),
            threads: Vec::default(),
            pool_size: num_cpus::get(),
            chan_size: None,
        }
    }

    /// Create a new pool, number of threads in this pool shall default to number of cores.
    /// Each threads shall be created with size-bounded input channel.
    pub fn new_sync(name: &str, chan_size: usize) -> Pool<Q, R, T> {
        Pool {
            name: name.to_string(),
            threads: Vec::default(),
            pool_size: num_cpus::get(),
            chan_size: Some(chan_size),
        }
    }

    pub fn set_pool_size(&mut self, pool_size: usize) -> &mut Self {
        self.pool_size = pool_size;
        self
    }

    /// Spawn all the threads configured for this pool.
    pub fn spawn<F, N>(&mut self, main_loop: F)
    where
        F: 'static + FnOnce(Rx<Q, R>) -> N + Send + Clone,
        N: 'static + Send + FnOnce() -> T,
        T: 'static + Send,
    {
        for i in 0..self.pool_size {
            let (name, main_loop) = (format!("{}-{}", self.name, i), main_loop.clone());
            let thread = match self.chan_size {
                Some(chan_size) => Thread::new_sync(&name, chan_size, main_loop),
                None => Thread::new(&name, main_loop),
            };
            self.threads.push(thread)
        }
    }

    /// Shutdown all threads, wait for them to exit and cleanup this pool.
    pub fn close_wait(self) -> Result<Vec<T>> {
        let mut results = vec![];
        for th in self.threads.into_iter() {
            results.push(th.join()?)
        }
        Ok(results)
    }
}

impl<Q, R, T> Pool<Q, R, T> {
    /// Return the name of the pool.
    pub fn to_name(&self) -> String {
        self.name.to_string()
    }

    /// Post a message to thread and don't wait for response.
    pub fn post(&self, msg: Q) -> Result<()> {
        let n: usize = rand::random::<usize>() % self.threads.len();
        let th: &Thread<Q, R, T> = &self.threads[n];

        th.to_tx().post(msg)
    }

    /// Send a request message to thread and wait for a response.
    pub fn request(&self, request: Q) -> Result<R> {
        let n: usize = rand::random::<usize>() % self.threads.len();
        let th: &Thread<Q, R, T> = &self.threads[n];

        th.to_tx().request(request)
    }

    /// Send a request message to thread and wait for a response.
    pub fn request_tx(&self, request: Q, rt_tx: mpsc::Sender<R>) -> Result<()> {
        let n: usize = rand::random::<usize>() % self.threads.len();
        let th: &Thread<Q, R, T> = &self.threads[n];

        th.to_tx().request_tx(request, rt_tx)
    }
}
