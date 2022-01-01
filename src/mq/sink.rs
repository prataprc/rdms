use std::{sync::mpsc, thread};

use crate::{mq, Error, Result};

pub struct Sink<Q, F>
where
    Q: 'static + Send + mq::Message,
    F: 'static + Send + Fn(Q) -> Result<bool>,
{
    name: String,
    chan_size: usize,

    input: Option<mpsc::Receiver<Q>>,
    callb: Option<F>,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl<Q, F> Sink<Q, F>
where
    Q: 'static + Send + mq::Message,
    F: 'static + Send + Fn(Q) -> Result<bool>,
{
    pub fn new_null(name: String, input: mpsc::Receiver<Q>) -> Self {
        Sink {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,

            input: Some(input),
            callb: None,
            handle: None,
        }
    }

    pub fn new_callb(name: String, input: mpsc::Receiver<Q>, callb: F) -> Self {
        Sink {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,

            input: Some(input),
            callb: Some(callb),
            handle: None,
        }
    }

    pub fn set_chan_size(&mut self, chan_size: usize) -> &mut Self {
        self.chan_size = chan_size;
        self
    }

    pub fn spawn(&mut self) {
        let chan_size = self.chan_size;

        let input = self.input.take().unwrap();
        self.handle = match self.callb.take() {
            Some(callb) => {
                Some(thread::spawn(move || action_callb(chan_size, input, callb)))
            }
            None => Some(thread::spawn(move || action_null(chan_size, input))),
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

fn action_null<Q>(chan_size: usize, input: mpsc::Receiver<Q>) -> Result<()>
where
    Q: 'static + Send,
{
    loop {
        match mq::get_messages(&input, chan_size) {
            Ok(_qmsgs) => (),
            Err(mpsc::TryRecvError::Disconnected) => break,
            _ => unreachable!(),
        }
    }

    Ok(())
}

fn action_callb<Q, F>(chan_size: usize, input: mpsc::Receiver<Q>, callb: F) -> Result<()>
where
    Q: 'static + Send + mq::Message,
    F: 'static + Send + Fn(Q) -> Result<bool>,
{
    let res = 'outer: loop {
        match mq::get_messages(&input, chan_size) {
            Ok(qmsgs) => {
                for qmsg in qmsgs.into_iter() {
                    match callb(qmsg) {
                        Ok(true) => (),
                        Ok(false) => break 'outer Ok(()),
                        Err(err) => break 'outer Err(err),
                    }
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => break Ok(()),
            _ => unreachable!(),
        }
    };

    callb(Q::finish())?;
    res
}
