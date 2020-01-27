use super::*;

#[test]
fn test_state() {
    let state: State = Default::default();
    assert_eq!(state.term, NIL_TERM);
    assert_eq!(state.committed, 0);
    assert_eq!(state.persisted, 0);
    assert_eq!(state.config, vec![]);
    assert_eq!(state.votedfor, DEFAULT_NODE.to_string());

    let state = State {
        term: 0x1234,
        committed: 0x2341,
        persisted: 0x3211,
        config: vec!["node1".to_string(), "node2".to_string()],
        votedfor: DEFAULT_NODE.to_string(),
    };

    let mut buf = vec![];
    assert_eq!(state.encode(&mut buf), 0);
    let mut dec_state: State = Default::default();
    assert_eq!(dec_state.decode(&buf), state);

    assert_eq!(dec_state.term, state.term);
    assert_eq!(dec_state.committed, state.committed);
    assert_eq!(dec_state.persisted, state.persisted);
    assert_eq!(dec_state.config, state.config);
    assert_eq!(dec_state.votedfor, state.votedfor);
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
    let mut res: Op<i32, i32> = Default::default();

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
