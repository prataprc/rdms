use std::{sync::mpsc, thread, time};

use crate::{mq, Error, Result};

#[derive(Clone)]
pub struct Config {
    pub name: String,
    pub chan_size: usize,
    pub deadline: Option<time::Instant>,
    pub timeout: Option<time::Duration>,
}

pub struct Sink {
    config: Config,
    handle: thread::JoinHandle<Result<()>>,
}

impl Sink {
    pub fn new_null<Q>(config: Config, input: mpsc::Receiver<Q>) -> Sink
    where
        Q: 'static + Send,
    {
        let handle = {
            let config = config.clone();
            thread::spawn(move || action_null(config.clone(), input))
        };

        Sink { config, handle }
    }

    pub fn new_callb<Q, F>(config: Config, input: mpsc::Receiver<Q>, callb: F) -> Sink
    where
        Q: 'static + Send + mq::Message,
        F: 'static + Send + FnMut(Q) -> Result<()>,
    {
        let handle = {
            let config = config.clone();
            thread::spawn(move || action_callb(config, input, callb))
        };

        Sink { config, handle }
    }

    /// Close this sink.
    pub fn close_wait(self) -> Result<()> {
        match self.handle.join() {
            Ok(res) => res,
            Err(_) => {
                err_at!(ThreadFail, msg: "thread fail Sink<{:?}>", self.config.name)
            }
        }
    }
}

fn action_null<Q>(config: Config, input: mpsc::Receiver<Q>) -> Result<()>
where
    Q: 'static + Send,
{
    let res = if let Some(deadline) = config.deadline {
        loop {
            match input.recv_deadline(deadline) {
                Ok(_msg) => (),
                Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    break err_at!(Timeout, msg: "thread Sink<{:?}>", config.name)
                }
            }
        }
    } else if let Some(timeout) = config.timeout {
        loop {
            match input.recv_timeout(timeout) {
                Ok(_msg) => (),
                Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    break err_at!(Timeout, msg: "thread Sink<{:?}>", config.name)
                }
            }
        }
    } else {
        loop {
            match input.recv() {
                Ok(_msg) => (),
                Err(_) => break Ok(()),
            }
        }
    };

    res
}

fn action_callb<Q, F>(
    config: Config,
    input: mpsc::Receiver<Q>,
    mut callb: F,
) -> Result<()>
where
    Q: 'static + Send + mq::Message,
    F: 'static + Send + FnMut(Q) -> Result<()>,
{
    let res = if let Some(deadline) = config.deadline {
        loop {
            match input.recv_deadline(deadline) {
                Ok(msg) => match callb(msg) {
                    Ok(_) => (),
                    err => break err,
                },
                Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    break err_at!(Timeout, msg: "thread Sink<{:?}>", config.name)
                }
            }
        }
    } else if let Some(timeout) = config.timeout {
        loop {
            match input.recv_timeout(timeout) {
                Ok(msg) => match callb(msg) {
                    Ok(_) => (),
                    err => break err,
                },
                Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    break err_at!(Timeout, msg: "thread Sink<{:?}>", config.name)
                }
            }
        }
    } else {
        loop {
            match input.recv() {
                Ok(msg) => match callb(msg) {
                    Ok(_) => (),
                    err => break err,
                },
                Err(_) => break Ok(()),
            }
        }
    };

    match callb(Q::finish()) {
        Ok(_) => res,
        err => err,
    }
}
