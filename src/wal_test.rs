use super::*;

#[test]
fn test_batch() {
    // encode/decode config
    let mut buf = vec![];
    let config = vec!["node1".to_string(), "node2".to_string()];
    let n = Batch::<i32, i32>::encode_config(&config, &mut buf);
    assert_eq!(n, 16);
    let (config_out, m) = Batch::<i32, i32>::decode_config(&buf).unwrap();
    assert_eq!(config, config_out);
    assert_eq!(n, m);
    // encode/decode votedfor
    let mut buf = vec![];
    let n = Batch::<i32, i32>::encode_votedfor("node1", &mut buf);
    assert_eq!(n, 7);
    let (votedfor, m) = Batch::<i32, i32>::decode_votedfor(&buf).unwrap();
    assert_eq!("node1", &votedfor);
    assert_eq!(n, m);

    // batch
    let mut batch: Batch<i32, i32> = Batch::new();

    let cnfg = vec![DEFAULT_NODE.to_string()];
    batch.set_config(&cnfg);
    match batch.clone() {
        Batch::Active { config, .. } => assert_eq!(cnfg, config),
        _ => unreachable!(),
    }

    batch.set_term(111, DEFAULT_NODE.to_string());
    match batch.clone() {
        Batch::Active { term, votedfor, .. } => {
            assert_eq!(term, 111);
            assert_eq!(votedfor, DEFAULT_NODE.to_string());
        }
        _ => unreachable!(),
    }

    batch.set_committed(1000);
    match batch.clone() {
        Batch::Active { committed, .. } => assert_eq!(committed, 1000),
        _ => unreachable!(),
    }

    batch.set_persisted(10000);
    match batch.clone() {
        Batch::Active { persisted, .. } => assert_eq!(persisted, 10000),
        _ => unreachable!(),
    }
    assert_eq!(batch.to_start_index(), None);
    assert_eq!(batch.to_last_index(), None);
    assert_eq!(batch.len(), 0);

    let (op1, op2, op3) = {
        (
            Op::new_set(10, 20),
            Op::new_set_cas(10, 30, 1),
            Op::new_delete(10),
        )
    };
    batch.add_entry(Entry::new_term(op1.clone(), 111, 1));
    batch.add_entry(Entry::new_term(op2.clone(), 111, 2));
    batch.add_entry(Entry::new_term(op3.clone(), 111, 3));

    assert_eq!(batch.to_start_index(), Some(1));
    assert_eq!(batch.to_last_index(), Some(3));
    assert_eq!(batch.len(), 3);
    assert_eq!(batch.clone().into_entries().len(), 3);

    // encode / decode active
    let mut buf = vec![];
    let n = batch.encode_active(&mut buf);
    assert_eq!(n, 253);
    let mut batch_out: Batch<i32, i32> = unsafe { mem::zeroed() };
    let m = batch_out
        .decode_active(&buf)
        .expect("failed decoder_active()");
    assert_eq!(n, m);
    match batch_out {
        Batch::Active {
            term: 111,
            committed: 1000,
            persisted: 10000,
            config,
            votedfor,
            entries,
        } => {
            assert_eq!(config, vec![DEFAULT_NODE.to_string()]);
            assert_eq!(votedfor, DEFAULT_NODE.to_string());
            assert_eq!(entries.len(), 3);
        }
        _ => unreachable!(),
    }
    // decode refer
    let mut batch_out: Batch<i32, i32> = unsafe { mem::zeroed() };
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
            assert_eq!(length, 253);
            assert_eq!(start_index, 1);
            assert_eq!(last_index, 3);
        }
        _ => unreachable!(),
    }
    assert_eq!(batch_out.to_start_index(), Some(1));
    assert_eq!(batch_out.to_last_index(), Some(3));
}

#[test]
fn test_entry() {
    // term
    let op = Op::new_set(10, 20);
    let r_entry = Entry::new_term(op, 23, 45);
    let mut buf = vec![];
    let n = r_entry.encode(&mut buf);
    assert_eq!(n, 48);
    match Entry::<i32, i32>::entry_type(&buf).unwrap() {
        EntryType::Term => (),
        _ => unreachable!(),
    }

    let mut entry: Entry<i32, i32> = unsafe { mem::zeroed() };
    entry.decode(&buf);
    match entry {
        Entry::Term {
            term: 23,
            index: 45,
            op: Op::Set { key: 10, value: 20 },
        } => (),
        _ => unreachable!(),
    }

    assert_eq!(r_entry.to_index(), 45);
    match r_entry.into_op() {
        Op::Set { key: 10, value: 20 } => (),
        _ => unreachable!(),
    }

    // client
    let op = Op::new_set(10, 20);
    let r_entry = Entry::new_client(op, 23, 45, 100, 200);
    let mut buf = vec![];
    let n = r_entry.encode(&mut buf);
    assert_eq!(n, 64);
    match Entry::<i32, i32>::entry_type(&buf).unwrap() {
        EntryType::Client => (),
        _ => unreachable!(),
    }

    let mut entry: Entry<i32, i32> = unsafe { mem::zeroed() };
    entry.decode(&buf);
    match entry {
        Entry::Client {
            term: 23,
            index: 45,
            id: 100,
            ceqno: 200,
            op: Op::Set { key: 10, value: 20 },
        } => (),
        _ => unreachable!(),
    }

    assert_eq!(r_entry.to_index(), 45);
    match r_entry.into_op() {
        Op::Set { key: 10, value: 20 } => (),
        _ => unreachable!(),
    }
}

#[test]
fn test_entry_term() {
    let mut buf = vec![];
    let r_op = Op::new_set(10, 20);
    let (r_term, r_index) = (23, 45);
    let n = Entry::encode_term(&r_op, r_term, r_index, &mut buf);
    assert_eq!(n, 48);

    let mut op: Op<i32, i32> = unsafe { mem::zeroed() };
    let mut term: u64 = 0;
    let mut index: u64 = 0;
    Entry::decode_term(&buf, &mut op, &mut term, &mut index);
    match op {
        Op::Set { key: 10, value: 20 } => (),
        _ => unreachable!(),
    }
    assert_eq!(r_term, term);
    assert_eq!(r_index, index);
}

#[test]
fn test_entry_client() {
    let mut buf = vec![];
    let r_op = Op::new_set(10, 20);
    let (r_term, r_index, r_id, r_ceqno) = (23, 45, 54, 65);
    let n = Entry::encode_client(&r_op, r_term, r_index, r_id, r_ceqno, &mut buf);
    assert_eq!(n, 64);

    let mut op: Op<i32, i32> = unsafe { mem::zeroed() };
    let mut term: u64 = 0;
    let mut index: u64 = 0;
    let mut id: u64 = 0;
    let mut ceqno: u64 = 0;
    Entry::decode_client(&buf, &mut op, &mut term, &mut index, &mut id, &mut ceqno);
    match op {
        Op::Set { key: 10, value: 20 } => (),
        _ => unreachable!(),
    }
    assert_eq!(r_term, term);
    assert_eq!(r_index, index);
    assert_eq!(r_id, id);
    assert_eq!(r_ceqno, ceqno);
}

#[test]
fn test_op_type() {
    let op_type: OpType = From::from(1_u64);
    assert_eq!(op_type, OpType::Set);
    let op_type: OpType = From::from(2_u64);
    assert_eq!(op_type, OpType::SetCAS);
    let op_type: OpType = From::from(3_u64);
    assert_eq!(op_type, OpType::Delete);
}

#[test]
fn test_op() {
    let mut out = vec![];
    let mut res: Op<i32, i32> = unsafe { mem::zeroed() };

    let op: Op<i32, i32> = Op::new_set(34, 43);
    op.encode(&mut out);
    assert_eq!(Op::<i32, i32>::op_type(&out).unwrap(), OpType::Set);
    let n = res.decode(&out).expect("op-set decode failed");
    assert_eq!(n, 24);
    match res {
        Op::Set { key: 34, value: 43 } => (),
        _ => unreachable!(),
    }

    let op: Op<i32, i32> = Op::new_set_cas(-34, -43, 100);
    out.resize(0, 0);
    op.encode(&mut out);
    assert_eq!(Op::<i32, i32>::op_type(&out).unwrap(), OpType::SetCAS);
    let n = res.decode(&out).expect("op-set-cas decode failed");
    assert_eq!(n, 32);
    match res {
        Op::SetCAS {
            key: -34,
            value: -43,
            cas: 100,
        } => (),
        _ => unreachable!(),
    }

    let op: Op<i32, i32> = Op::new_delete(34);
    out.resize(0, 0);
    op.encode(&mut out);
    assert_eq!(Op::<i32, i32>::op_type(&out).unwrap(), OpType::Delete);
    let n = res.decode(&out).expect("op-delete decode failed");
    assert_eq!(n, 12);
    match res {
        Op::Delete { key: 34 } => (),
        _ => unreachable!(),
    }
}
