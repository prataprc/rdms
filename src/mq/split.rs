use std::{sync::mpsc, thread, time};

use crate::{mq, Error, Result};

pub struct Split<Q>
where
    Q: 'static + Send + Clone,
{
    name: String,
    chan_size: usize,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,
    n: usize,

    input: Option<mpsc::Receiver<Q>>,
    handle: Option<thread::JoinHandle<Result<()>>>,
}

impl<Q> Split<Q>
where
    Q: 'static + Send + Clone,
{
    pub fn new(name: String, input: mpsc::Receiver<Q>, n: usize) -> Self {
        Split {
            name,
            chan_size: mq::DEFAULT_CHAN_SIZE,
            deadline: None,
            timeout: None,
            n,

            input: Some(input),
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

    pub fn spawn(&mut self) -> Vec<mpsc::Receiver<Q>> {
        let name = self.name.clone();
        let (deadline, timeout) = (self.deadline.clone(), self.timeout.clone());
        let (mut txs, mut outputs) = (vec![], vec![]);
        (0..self.n).for_each(|_| {
            let (tx, output) = mpsc::sync_channel(self.chan_size);
            txs.push(tx);
            outputs.push(output);
        });

        let input = self.input.take().unwrap();
        self.handle = Some(thread::spawn(move || {
            action(name, deadline, timeout, input, txs)
        }));

        outputs
    }

    pub fn close_wait(self) -> Result<()> {
        match self.handle {
            Some(handle) => match handle.join() {
                Ok(res) => res,
                Err(_) => {
                    err_at!(ThreadFail, msg: "thread fail Split<{:?}>", self.name)
                }
            },
            None => Ok(()),
        }
    }
}

fn action<Q>(
    name: String,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,
    input: mpsc::Receiver<Q>,
    txs: Vec<mpsc::SyncSender<Q>>,
) -> Result<()>
where
    Q: 'static + Send + Clone,
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
                for tx in txs.iter() {
                    err_at!(IPCFail, tx.send(msg.clone()), "thread Split<{:?}>", name)?;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                err_at!(Timeout, msg: "thread Split<{:?}>", name)?
            }
        }
    }

    // tx shall be dropped here.
    Ok(())
}
