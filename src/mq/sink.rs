use std::{sync::mpsc, thread, time};

use crate::{mq, Error, Result};

pub struct Sink<Q, F>
where
    Q: 'static + Send + mq::Message,
    F: 'static + Send + FnMut(Q) -> Result<bool>,
{
    name: String,
    chan_size: usize,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,

    input: Option<mpsc::Receiver<Q>>,
    callb: Option<F>,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl<Q, F> Sink<Q, F>
where
    Q: 'static + Send + mq::Message,
    F: 'static + Send + FnMut(Q) -> Result<bool>,
{
    pub fn new_null(name: String, input: mpsc::Receiver<Q>) -> Self {
        Sink {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,
            deadline: None,
            timeout: None,

            input: Some(input),
            callb: None,
            handle: None,
        }
    }

    pub fn new_callb(name: String, input: mpsc::Receiver<Q>, callb: F) -> Self {
        Sink {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,
            deadline: None,
            timeout: None,

            input: Some(input),
            callb: Some(callb),
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

    pub fn spawn(&mut self) {
        let name = self.name.clone();
        let (deadline, timeout) = (self.deadline.clone(), self.timeout.clone());

        let input = self.input.take().unwrap();
        self.handle = match self.callb.take() {
            Some(callb) => Some(thread::spawn(move || {
                action_callb(name, deadline, timeout, input, callb)
            })),
            None => Some(thread::spawn(move || {
                action_null(name, deadline, timeout, input)
            })),
        };
    }

    /// Close this sink.
    pub fn close_wait(self) -> Result<()> {
        match self.handle {
            Some(handle) => match handle.join() {
                Ok(res) => res,
                Err(_) => {
                    err_at!(ThreadFail, msg: "thread fail Sink<{:?}>", self.name)
                }
            },
            None => Ok(()),
        }
    }
}

fn action_null<Q>(
    name: String,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,
    input: mpsc::Receiver<Q>,
) -> Result<()>
where
    Q: 'static + Send,
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
            Ok(_msg) => (),
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                err_at!(Timeout, msg: "thread Sink<{:?}>", name)?
            }
        }
    }

    Ok(())
}

fn action_callb<Q, F>(
    name: String,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,
    input: mpsc::Receiver<Q>,
    mut callb: F,
) -> Result<()>
where
    Q: 'static + Send + mq::Message,
    F: 'static + Send + FnMut(Q) -> Result<bool>,
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
            Ok(msg) => match callb(msg)? {
                true => (),
                false => break,
            },
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                err_at!(Timeout, msg: "thread Sink<{:?}>", name)?
            }
        }
    }

    callb(Q::finish())?;
    Ok(())
}
