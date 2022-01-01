use std::{result, sync::mpsc};

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

fn get_messages<Q>(
    input: &mpsc::Receiver<Q>,
    chan_size: usize,
) -> result::Result<Vec<Q>, mpsc::TryRecvError>
where
    Q: 'static + Send,
{
    let mut qmsgs = vec![];
    loop {
        match input.try_recv() {
            Ok(qmsg) if qmsgs.len() < chan_size => qmsgs.push(qmsg),
            Ok(qmsg) => {
                qmsgs.push(qmsg);
                break Ok(qmsgs);
            }
            Err(mpsc::TryRecvError::Empty) => break Ok(qmsgs),
            Err(err @ mpsc::TryRecvError::Disconnected) => break Err(err),
        }
    }
}

//fn put_messages<R>(
//    rmsgs: &mut Vec<Res<R>>,
//    mut rseqno: u64,
//    tx: &mpsc::SyncSender<R>,
//) -> result::Result<u64, mpsc::SendError<R>>
//where
//    R: 'static + Send,
//{
//    rmsgs.sort_unstable_by_key(|m| m.seqno);
//    rmsgs.reverse();
//
//    while let Some(rmsg) = rmsgs.pop() {
//        if rmsg.seqno == rseqno {
//            rseqno += 1;
//            tx.send(rmsg.rmsg)?;
//        } else {
//            rmsgs.push(rmsg);
//            break;
//        }
//    }
//
//    Ok(rseqno)
//}
