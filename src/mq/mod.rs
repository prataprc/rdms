use std::{result, sync::mpsc, time};

pub mod filter;
pub mod filter_map;
pub mod map;
pub mod reduce;
pub mod sink;
pub mod source;
pub mod split;

const DEFAULT_CHAN_SIZE: usize = 1024;

pub trait Message {
    fn finish() -> Self;
}

struct Req<Q> {
    seqno: u64,
    qmsg: Q,
}

impl<Q> Req<Q> {
    fn new(seqno: u64, qmsg: Q) -> Self {
        Req { seqno, qmsg }
    }
}

struct Res<R> {
    seqno: u64,
    rmsg: R,
}

impl<R> Res<R> {
    fn new(seqno: u64, rmsg: R) -> Self {
        Res { seqno, rmsg }
    }
}

fn get_message<Q>(
    input: &mpsc::Receiver<Q>,
    deadline: Option<time::Instant>,
    timeout: Option<time::Duration>,
) -> result::Result<Q, mpsc::RecvTimeoutError>
where
    Q: 'static + Send,
{
    if let Some(deadline) = deadline {
        input.recv_deadline(deadline)
    } else if let Some(timeout) = timeout {
        input.recv_timeout(timeout)
    } else {
        match input.recv() {
            Ok(msg) => Ok(msg),
            Err(_) => Err(mpsc::RecvTimeoutError::Disconnected),
        }
    }
}

fn put_messages<R>(
    rmsgs: &mut Vec<Res<R>>,
    mut rseqno: u64,
    tx: &mpsc::SyncSender<R>,
) -> result::Result<u64, mpsc::SendError<R>>
where
    R: 'static + Send,
{
    rmsgs.sort_unstable_by_key(|m| m.seqno);
    rmsgs.reverse();

    while let Some(rmsg) = rmsgs.pop() {
        if rmsg.seqno == rseqno {
            rseqno += 1;
            tx.send(rmsg.rmsg)?;
        } else {
            rmsgs.push(rmsg);
            break;
        }
    }

    Ok(rseqno)
}
