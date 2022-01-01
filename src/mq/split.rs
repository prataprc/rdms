use std::{sync::mpsc, thread};

use crate::{mq, Error, Result};

pub struct Split<Q>
where
    Q: 'static + Send + Clone,
{
    name: String,
    chan_size: usize,
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
            n,

            input: Some(input),
            handle: None,
        }
    }

    pub fn set_chan_size(&mut self, chan_size: usize) -> &mut Self {
        self.chan_size = chan_size;
        self
    }

    pub fn spawn(&mut self) -> Vec<mpsc::Receiver<Q>> {
        let (name, chan_size) = (self.name.clone(), self.chan_size);
        let (mut txs, mut outputs) = (vec![], vec![]);

        (0..self.n).for_each(|_| {
            let (tx, output) = mpsc::sync_channel(self.chan_size);
            txs.push(tx);
            outputs.push(output);
        });

        let input = self.input.take().unwrap();
        self.handle = Some(thread::spawn(move || action(name, chan_size, input, txs)));

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
    chan_size: usize,
    input: mpsc::Receiver<Q>,
    txs: Vec<mpsc::SyncSender<Q>>,
) -> Result<()>
where
    Q: 'static + Send + Clone,
{
    loop {
        match mq::get_messages(&input, chan_size) {
            Ok(qmsgs) => {
                for qmsg in qmsgs.into_iter() {
                    for tx in txs.iter() {
                        err_at!(
                            IPCFail,
                            tx.send(qmsg.clone()),
                            "thread Split<{:?}>",
                            name
                        )?;
                    }
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => break,
            _ => unreachable!(),
        }
    }

    Ok(())
}
