use std::ffi::OsStr;

use super::*;

#[test]
fn test_wal_initial() {
    let dir = {
        let mut dir_path = path::PathBuf::new();
        dir_path.push(std::env::temp_dir().into_os_string());
        dir_path.push("test_wal");
        let dir: &OsStr = dir_path.as_ref();
        dir.clone().to_os_string()
    };
    fs::remove_dir_all(&dir).ok();
    fs::create_dir_all(&dir);

    let nshards = 1;
    let walo = Wal::create(dir.clone(), "users".to_string(), nshards);
    let mut walo = walo.unwrap();
    walo.set_journal_limit(40000);
    let w = walo.spawn_writer().unwrap();

    let ops = write_wal(&w);
    assert_eq!(ops.len(), 610);

    validate_journals1_file1(dir.clone(), ops.clone());
    validate_journals1_file2(dir.clone(), ops.clone());
    validate_journals1_file3(dir.clone(), ops.clone());
}

#[test]
fn test_journal_file() {
    let file_path = Journal::<i32, i32>::parts_to_file_name("users", 1, 1);
    let file: &OsStr = file_path.as_ref();

    assert_eq!(file_path, "users-wal-shard-1-journal-1.wal".to_string());

    match Journal::<i32, i32>::file_name_to_parts(&file.to_os_string()) {
        Some((name, shard_id, num)) => {
            assert_eq!(name, "users".to_string());
            assert_eq!(shard_id, 1);
            assert_eq!(num, 1);
        }
        None => unreachable!(),
    }
}

#[test]
fn test_journal() {
    let dir = std::env::temp_dir().into_os_string();
    let (name, shard_id, num) = ("users".to_string(), 1, 1);
    let mut j = Journal::<i32, i32>::create(
        // create a new journal
        dir.clone(),
        name.clone(),
        shard_id,
        num,
    )
    .expect("failed to create journal file for users");

    assert_eq!(j.shard_id(), 1);
    assert_eq!(j.to_current_term(), NIL_TERM);
    assert_eq!(j.to_start_index(), None);
    assert_eq!(j.to_last_index(), None);
    assert!(!j.exceed_limit(0).expect("exceed limit"));

    let (tx, rx) = mpsc::sync_channel(1);

    j.append_op(1, OpRequest::new_set(10, 2000, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(1)));

    j.append_op(2, OpRequest::new_set(20, 2001, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(2)));

    j.append_op(3, OpRequest::new_set(30, 2002, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(3)));

    j.append_op(4, OpRequest::new_set_cas(10, 3000, 1, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(4)));

    j.append_op(5, OpRequest::new_set_cas(20, 3001, 2, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(5)));

    j.append_op(6, OpRequest::new_delete(30, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(6)));

    assert!(j.flush1(10 * 1024).unwrap().is_none());
    assert_eq!(j.fd.as_ref().unwrap().metadata().unwrap().len(), 431);

    j.append_op(7, OpRequest::new_set(40, 2000, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(7)));

    j.append_op(8, OpRequest::new_set(30, 5000, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(8)));

    assert!(j.flush1(10 * 1024).unwrap().is_none());
    assert_eq!(j.fd.as_ref().unwrap().metadata().unwrap().len(), 431 + 235);

    j.append_op(9, OpRequest::new_set(50, 2002, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(9)));

    j.append_op(10, OpRequest::new_set_cas(10, 5000, 6, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(10)));

    j.append_op(11, OpRequest::new_set_cas(50, 3001, 9, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(11)));

    j.append_op(12, OpRequest::new_delete(10, tx.clone()))
        .unwrap();
    assert!(rx.recv().unwrap() == Opresp::Result(Ok(12)));

    assert!(j.flush1(10 * 1024).unwrap().is_none());
    assert_eq!(
        j.fd.as_ref().unwrap().metadata().unwrap().len(),
        431 + 235 + 335
    );

    let verify_fn = |j: Journal<i32, i32>| {
        assert_eq!(j.shard_id(), 1);
        assert_eq!(j.to_start_index(), Some(1));
        assert_eq!(j.to_last_index(), Some(12));
        assert_eq!(j.to_current_term(), NIL_TERM);
        assert_eq!(j.exceed_limit(1000).expect("exceed limit"), true);
        assert_eq!(j.exceed_limit(1001).expect("exceed limit"), false);
        assert_eq!(j.exceed_limit(1002).expect("exceed limit"), false);

        for (i, entry) in j.into_iter().unwrap().enumerate() {
            match (i, entry.unwrap()) {
                (0, entry) => {
                    let e = Entry::new_term(Op::new_set(10, 2000), NIL_TERM, 1);
                    assert!(e == entry)
                }
                (1, entry) => {
                    let e = Entry::new_term(Op::new_set(20, 2001), NIL_TERM, 2);
                    assert!(e == entry)
                }
                (2, entry) => {
                    let e = Entry::new_term(Op::new_set(30, 2002), NIL_TERM, 3);
                    assert!(e == entry)
                }
                (3, entry) => {
                    let op = Op::new_set_cas(10, 3000, 1);
                    let e = Entry::new_term(op, NIL_TERM, 4);
                    assert!(e == entry)
                }
                (4, entry) => {
                    let op = Op::new_set_cas(20, 3001, 2);
                    let e = Entry::new_term(op, NIL_TERM, 5);
                    assert!(e == entry)
                }
                (5, entry) => {
                    let e = Entry::new_term(Op::new_delete(30), NIL_TERM, 6);
                    assert!(e == entry)
                }
                // next batch
                (6, entry) => {
                    let e = Entry::new_term(Op::new_set(40, 2000), NIL_TERM, 7);
                    assert!(e == entry)
                }
                (7, entry) => {
                    let e = Entry::new_term(Op::new_set(30, 5000), NIL_TERM, 8);
                    assert!(e == entry)
                }
                // next batch
                (8, entry) => {
                    let e = Entry::new_term(Op::new_set(50, 2002), NIL_TERM, 9);
                    assert!(e == entry)
                }
                (9, entry) => {
                    let op = Op::new_set_cas(10, 5000, 6);
                    let e = Entry::new_term(op, NIL_TERM, 10);
                    assert!(e == entry)
                }
                (10, entry) => {
                    let op = Op::new_set_cas(50, 3001, 9);
                    let e = Entry::new_term(op, NIL_TERM, 11);
                    assert!(e == entry)
                }
                (11, entry) => {
                    let e = Entry::new_term(Op::new_delete(10), NIL_TERM, 12);
                    assert!(e == entry)
                }
                _ => unreachable!(),
            }
        }
    };

    verify_fn(j);

    // load test case
    let file = {
        let mut file_path = path::PathBuf::new();
        file_path.push(dir);
        file_path.push(Journal::<i32, i32>::parts_to_file_name(
            &name, shard_id, num,
        ));
        let file: &OsStr = file_path.as_ref();
        file.clone().to_os_string()
    };

    let j = Journal::<i32, i32>::load(name, file);
    let mut j = j.unwrap().unwrap();
    j.open().expect("unable to open journal file");
    verify_fn(j);

    // TODO: purge()
}

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
    assert_eq!(n, 293);
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
            assert_eq!(length, 293);
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
    let n = r_entry.encode(&mut buf);
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
    let n = Entry::encode_term(&r_op, r_term, r_index, &mut buf);
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
    let n = Entry::encode_client(&r_op, r_term, r_index, r_id, r_ceqno, &mut buf);
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

#[derive(Clone)]
struct TestWriteOp {
    index: u64,
    op: Op<i32, i32>,
}

impl Ord for TestWriteOp {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.index.cmp(&other.index)
    }
}

impl Eq for TestWriteOp {}

impl PartialOrd for TestWriteOp {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.index.partial_cmp(&other.index)
    }
}

impl PartialEq for TestWriteOp {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

fn write_wal(w: &Writer<i32, i32>) -> Vec<TestWriteOp> {
    let mut ops = vec![];
    for key in 1..=300_i32 {
        let value = key * 10;
        let index = w.set(key, value).unwrap();
        ops.push(TestWriteOp {
            index,
            op: Op::Set { key, value },
        });
    }
    for key in 1..=300_i32 {
        let value = key * 100;
        let i: usize = key.try_into().unwrap();
        let cas = ops[i - 1].index;
        let index = w.set_cas(key, value, cas).unwrap();
        ops.push(TestWriteOp {
            index,
            op: Op::SetCAS { key, value, cas },
        });
    }
    for key in 1..=10_i32 {
        let key = key & 3;
        let index = w.delete(&key).unwrap();
        ops.push(TestWriteOp {
            index,
            op: Op::Delete { key },
        });
    }
    ops
}

fn validate_journals1_file1(dir: ffi::OsString, ops: Vec<TestWriteOp>) {
    let file = {
        let mut file = path::PathBuf::new();
        file.push(dir);
        file.push("users-wal-shard-1-journal-1.wal".to_string());
        file.as_path().as_os_str().to_os_string()
    };
    let mut j = Journal::<i32, i32>::load("users".to_string(), file.clone())
        .unwrap()
        .unwrap();
    j.open().expect("unable to open journal file");
    assert_eq!(j.shard_id(), 1);
    assert_eq!(j.to_current_term(), NIL_TERM);
    let a = j.to_start_index().unwrap() as usize;
    assert_eq!(a, 1);
    let z = j.to_last_index().unwrap() as usize;
    assert_eq!(z, 213);
    assert_eq!(j.exceed_limit(40000).expect("exceed limit"), false);

    let ref_ops: Vec<(usize, Op<i32, i32>)> = ops[(a - 1)..(z - 1)]
        .iter()
        .enumerate()
        .map(|(i, op)| (a + i, op.op.clone()))
        .collect();
    let iter = j.into_iter().unwrap().zip(ref_ops.into_iter());
    for (entry, (index, ref_op)) in iter {
        let e = Entry::new_term(ref_op, NIL_TERM, index as u64);
        assert!(e == entry.unwrap())
    }
}

fn validate_journals1_file2(dir: ffi::OsString, ops: Vec<TestWriteOp>) {
    let file = {
        let mut file = path::PathBuf::new();
        file.push(dir);
        file.push("users-wal-shard-1-journal-2.wal".to_string());
        file.as_path().as_os_str().to_os_string()
    };
    let mut j = Journal::<i32, i32>::load("users".to_string(), file.clone())
        .unwrap()
        .unwrap();
    j.open().expect("unable to open journal file");
    assert_eq!(j.shard_id(), 1);
    assert_eq!(j.to_current_term(), NIL_TERM);
    let a = j.to_start_index().unwrap() as usize;
    assert_eq!(a, 214);
    let z = j.to_last_index().unwrap() as usize;
    assert_eq!(z, 421);
    assert_eq!(j.exceed_limit(40000).expect("exceed limit"), false);

    let ref_ops: Vec<(usize, Op<i32, i32>)> = ops[(a - 1)..(z - 1)]
        .iter()
        .enumerate()
        .map(|(i, op)| (a + i, op.op.clone()))
        .collect();
    let iter = j.into_iter().unwrap().zip(ref_ops.into_iter());
    for (entry, (index, ref_op)) in iter {
        let e = Entry::new_term(ref_op, NIL_TERM, index as u64);
        assert!(e == entry.unwrap())
    }
}

fn validate_journals1_file3(dir: ffi::OsString, ops: Vec<TestWriteOp>) {
    let file = {
        let mut file = path::PathBuf::new();
        file.push(dir);
        file.push("users-wal-shard-1-journal-3.wal".to_string());
        file.as_path().as_os_str().to_os_string()
    };
    let mut j = Journal::<i32, i32>::load("users".to_string(), file.clone())
        .unwrap()
        .unwrap();
    j.open().expect("unable to open journal file");
    assert_eq!(j.shard_id(), 1);
    assert_eq!(j.to_current_term(), NIL_TERM);
    let a = j.to_start_index().unwrap() as usize;
    assert_eq!(a, 422);
    let z = j.to_last_index().unwrap() as usize;
    assert_eq!(z, 610);
    assert_eq!(j.exceed_limit(40000).expect("exceed limit"), false);

    let ref_ops: Vec<(usize, Op<i32, i32>)> = ops[(a - 1)..(z - 1)]
        .iter()
        .enumerate()
        .map(|(i, op)| (a + i, op.op.clone()))
        .collect();
    let iter = j.into_iter().unwrap().zip(ref_ops.into_iter());
    for (entry, (index, ref_op)) in iter {
        let e = Entry::new_term(ref_op, NIL_TERM, index as u64);
        assert!(e == entry.unwrap())
    }
}
