use std::{mem, sync::mpsc, thread, time};

use crate::{Error, Result};

#[derive(Clone)]
pub struct Config {
    pub name: String,
    pub chan_size: usize,
    pub deadline: Option<time::Instant>,
    pub timeout: Option<time::Duration>,
}

pub struct Filter<Q>
where
    Q: 'static + Send,
{
    config: Config,
    handle: thread::JoinHandle<Result<()>>,
    output: Option<mpsc::Receiver<Q>>,
}

impl<Q> Filter<Q>
where
    Q: 'static + Send,
{
    pub fn new<F>(config: Config, input: mpsc::Receiver<Q>, filter: F) -> Filter<Q>
    where
        Q: 'static + Send,
        F: 'static + Send + FnMut(&Q) -> Result<bool>,
    {
        let (handle, output) = {
            let config = config.clone();
            let (tx, output) = mpsc::sync_channel(config.chan_size);
            let handle = thread::spawn(move || action(config, input, tx, filter));
            (handle, Some(output))
        };

        Filter {
            config,
            handle,
            output,
        }
    }

    pub fn output(&mut self) -> mpsc::Receiver<Q> {
        self.output.take().unwrap()
    }

    pub fn close_wait(self) -> Result<()> {
        match self.handle.join() {
            Ok(res) => res,
            Err(_) => {
                err_at!(ThreadFail, msg: "thread fail Filter<{:?}>", self.config.name)
            }
        }
    }
}

fn action<Q, F>(
    config: Config,
    input: mpsc::Receiver<Q>,
    tx: mpsc::SyncSender<Q>,
    mut filter: F,
) -> Result<()>
where
    Q: 'static + Send,
    F: 'static + Send + FnMut(&Q) -> Result<bool>,
{
    let mut iter = input.iter();
    loop {
        match iter.next() {
            Some(msg) => match filter(&msg)? {
                true => {
                    err_at!(IPCFail, tx.send(msg), "thread Filter<{:?}>", config.name)?
                }
                false => (),
            },
            None => break,
        }
    }

    mem::drop(tx);

    Ok(())
}
