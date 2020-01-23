use super::*;

#[test]
fn test_entry() {
    // term
    let op = Op::new_set(10, 20);
    let r_entry = Entry::new_term(op, 23, 45);
    let mut buf = vec![];
    let n = r_entry.encode(&mut buf).unwrap();
    assert_eq!(n, 48);
    match Entry::<i32, i32>::entry_type(&buf).unwrap() {
        EntryType::Term => (),
        _ => unreachable!(),
    }

    let mut entry: Entry<i32, i32> = unsafe { mem::zeroed() };
    entry.decode(&buf).unwrap();
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
    let n = r_entry.encode(&mut buf).unwrap();
    assert_eq!(n, 64);
    match Entry::<i32, i32>::entry_type(&buf).unwrap() {
        EntryType::Client => (),
        _ => unreachable!(),
    }

    let mut entry: Entry<i32, i32> = unsafe { mem::zeroed() };
    entry.decode(&buf).unwrap();
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
    let n = Entry::encode_term(&r_op, r_term, r_index, &mut buf).unwrap();
    assert_eq!(n, 48);

    let mut op: Op<i32, i32> = unsafe { mem::zeroed() };
    let mut term: u64 = 0;
    let mut index: u64 = 0;
    Entry::decode_term(&buf, &mut op, &mut term, &mut index).unwrap();
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
    let n = Entry::encode_client(&r_op, r_term, r_index, r_id, r_ceqno, &mut buf).unwrap();
    assert_eq!(n, 64);

    let mut op: Op<i32, i32> = unsafe { mem::zeroed() };
    let mut term: u64 = 0;
    let mut index: u64 = 0;
    let mut id: u64 = 0;
    let mut ceqno: u64 = 0;
    Entry::decode_client(
        // all mutable reference
        &buf, &mut op, &mut term, &mut index, &mut id, &mut ceqno,
    )
    .unwrap();
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
    op.encode(&mut out).unwrap();
    assert_eq!(Op::<i32, i32>::op_type(&out).unwrap(), OpType::Set);
    let n = res.decode(&out).expect("op-set decode failed");
    assert_eq!(n, 24);
    match res {
        Op::Set { key: 34, value: 43 } => (),
        _ => unreachable!(),
    }

    let op: Op<i32, i32> = Op::new_set_cas(-34, -43, 100);
    out.resize(0, 0);
    op.encode(&mut out).unwrap();
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
    op.encode(&mut out).unwrap();
    assert_eq!(Op::<i32, i32>::op_type(&out).unwrap(), OpType::Delete);
    let n = res.decode(&out).expect("op-delete decode failed");
    assert_eq!(n, 12);
    match res {
        Op::Delete { key: 34 } => (),
        _ => unreachable!(),
    }
}
