use super::*;

use crate::dlog_wal;

#[test]
fn test_entry() {
    // term
    let r_entry = Entry::new(45, dlog_wal::Op::new_set(10, 20));

    let mut buf = vec![];
    let n = r_entry.encode(&mut buf).unwrap();
    assert_eq!(n, 32);
    let mut entry: Entry<dlog_wal::Op<i32, i32>> = Default::default();
    entry.decode(&buf).unwrap();
    assert_eq!(entry.index, r_entry.index);
    assert_eq!(entry.op, r_entry.op);

    assert_eq!(r_entry.to_index(), 45);
    match r_entry.into_op() {
        dlog_wal::Op::Set { key: 10, value: 20 } => (),
        _ => unreachable!(),
    }
}

#[test]
fn test_batch1() {
    // batch
    let mut batch: Batch<dlog_wal::State, dlog_wal::Op> = Batch::default_active();

    assert_eq!(batch.to_start_index(), None);
    assert_eq!(batch.to_last_index(), None);
    assert_eq!(batch.len(), 0);

    let (op1, op2, op3) = {
        (
            dlog_wal::Op::new_set(10, 20),
            dlog_wal::Op::new_set_cas(10, 30, 1),
            dlog_wal::Op::new_delete(10),
        )
    };
    batch.add_entry(Entry::new_term(1, op1.clone()));
    batch.add_entry(Entry::new_term(2, op2.clone()));
    batch.add_entry(Entry::new_term(3, op3.clone()));

    assert_eq!(batch.to_start_index(), Some(1));
    assert_eq!(batch.to_last_index(), Some(3));
    assert_eq!(batch.len(), 3);
    assert_eq!(batch.clone().into_entries().len(), 3);

    // encode / decode active
    let mut buf = vec![];
    let n = batch.encode_active(&mut buf).unwrap();
    assert_eq!(n, 293);

    let mut batch_out: Batch<dlog_wal::State, dlog_wal::Op> = Batch::default_active();
    let m = batch_out
        .decode_active(&buf)
        .expect("failed decoder_active()");

    assert!(batch == batch_out);

    // decode refer
    let mut batch_out: Batch<dlog_wal::State, dlog_wal::Op> = Batch::default_active();
    let m = batch_out
        .decode_refer(&buf, 12345678)
        .expect("failed decoder_active()");
    assert_eq!(n, m);

    match batch_out {
        Batch::Refer {
            fpos,
            length,
            start_index,
            last_index,
        } => {
            assert_eq!(fpos, 12345678);
            assert_eq!(length, 293);
            assert_eq!(start_index, 1);
            assert_eq!(last_index, 3);
        }
        _ => unreachable!(),
    }

    assert_eq!(batch_out.to_start_index(), Some(1));
    assert_eq!(batch_out.to_last_index(), Some(3));
}
