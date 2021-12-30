use std::{sync::mpsc, thread, time};

use crate::{mq, Error, Result};

#[derive(Clone)]
pub struct Config {
    pub name: String,
    pub chan_size: usize,
    pub deadline: Option<time::Instant>,
    pub timeout: Option<time::Duration>,
}

pub struct Reduce<Q, R, F>
where
    Q: 'static + Send,
    R: 'static + Send,
    F: 'static + Send + FnMut(R, Q) -> Result<R>,
{
    name: String,
    chan_size: usize,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,

    input: Option<mpsc::Receiver<Q>>,
    initial: Option<R>,
    reduce: Option<F>,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl<Q, R, F> Reduce<Q, R, F>
where
    Q: 'static + Send,
    R: 'static + Send,
    F: 'static + Send + FnMut(R, Q) -> Result<R>,
{
    pub fn new(name: String, input: mpsc::Receiver<Q>, initial: R, reduce: F) -> Self {
        Reduce {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,
            deadline: None,
            timeout: None,

            input: Some(input),
            initial: Some(initial),
            reduce: Some(reduce),
            handle: None,
        }
    }

    pub fn set_chan_size(&mut self, chan_size: usize) -> &mut Self {
        self.chan_size = chan_size;
        self
    }

    pub fn set_deadline(&mut self, deadline: time::Instant) -> &mut Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn set_timeout(&mut self, timeout: time::Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn spawn(&mut self) -> mpsc::Receiver<R> {
        let name = self.name.clone();
        let (deadline, timeout) = (self.deadline, self.timeout);
        let (tx, output) = mpsc::sync_channel(self.chan_size);

        let input = self.input.take().unwrap();
        let initial = self.initial.take().unwrap();
        let reduce = self.reduce.take().unwrap();
        self.handle = Some(thread::spawn(move || {
            action(name, deadline, timeout, input, tx, initial, reduce)
        }));

        output
    }

    pub fn close_wait(self) -> Result<()> {
        match self.handle {
            Some(handle) => match handle.join() {
                Ok(res) => res,
                Err(_) => {
                    err_at!(ThreadFail, msg: "thread fail Reduce<{:?}>", self.name)
                }
            },
            None => Ok(()),
        }
    }
}

fn action<Q, R, F>(
    name: String,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,
    input: mpsc::Receiver<Q>,
    tx: mpsc::SyncSender<R>,
    mut initial: R,
    mut reduce: F,
) -> Result<()>
where
    R: 'static + Send,
    Q: 'static + Send,
    F: 'static + Send + FnMut(R, Q) -> Result<R>,
{
    loop {
        let res = if let Some(deadline) = deadline {
            input.recv_deadline(deadline)
        } else if let Some(timeout) = timeout {
            input.recv_timeout(timeout)
        } else {
            match input.recv() {
                Ok(msg) => Ok(msg),
                Err(_) => Err(mpsc::RecvTimeoutError::Disconnected),
            }
        };

        match res {
            Ok(msg) => {
                initial = reduce(initial, msg)?;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                err_at!(Timeout, msg: "thread Reduce<{:?}>", name)?
            }
        }
    }

    err_at!(IPCFail, tx.send(initial), "thread Reduce<{:?}>", name)?;

    // tx shall be dropped here.
    Ok(())
}
