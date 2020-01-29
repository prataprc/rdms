use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;
use crate::wal;

#[test]
fn test_journal_file() {
    let name = "my-dlog".to_string();
    let typ = "wal".to_string();
    let (shard_id, num) = (10, 1);

    let journal_file: JournalFile = (name.clone(), typ.clone(), shard_id, num).into();
    assert_eq!(
        journal_file.clone().0.into_string().unwrap(),
        "my-dlog-wal-shard-10-journal-1.dlog".to_string()
    );

    let journal_file = journal_file.next();
    assert_eq!(
        journal_file.clone().0.into_string().unwrap(),
        "my-dlog-wal-shard-10-journal-2.dlog".to_string()
    );

    let (nm, t, id, num) = journal_file.try_into().unwrap();
    assert_eq!(nm, name);
    assert_eq!(t, typ);
    assert_eq!(shard_id, id);
    assert_eq!(num, 2);
}

#[test]
fn test_journal_active() {
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let dir = {
        let mut dir = path::PathBuf::new();
        dir.push(std::env::temp_dir());
        dir.push("test-journal-active");
        dir.into_os_string()
    };
    fs::create_dir_all(&dir).unwrap();

    let mut journal: Journal<wal::State, wal::Op<i64, i64>> = {
        let name = "journal-active".to_string();
        Journal::new_active(dir, name, 1, 1).unwrap()
    };
    let limit = 1_000_000_000;

    for i in 0..100 {
        for j in 0..1000 {
            let op = wal::Op::<i64, i64>::new_set(10 * i + j, 20 + i);
            let index = (i * 1000 + j) as u64 + 1;
            journal.add_entry(DEntry::new(index, op));
        }
        let nosync: bool = rng.gen();
        assert_eq!(journal.flush1(limit, nosync).unwrap().is_none(), true);
    }

    assert_eq!(journal.to_last_index().unwrap(), 100_000);
    let rf: &ffi::OsStr = "journal-active-wal-shard-1-journal-1.dlog".as_ref();
    assert_eq!(
        path::Path::new(&journal.to_file_path())
            .file_name()
            .unwrap(),
        rf
    );
    assert_eq!(journal.is_cold(), false);

    let mut fd = {
        let file_path = journal.to_file_path();
        let mut opts = fs::OpenOptions::new();
        opts.read(true).open(&file_path).unwrap()
    };
    for (i, batch) in journal.into_batches().unwrap().into_iter().enumerate() {
        let batch = batch.into_active(&mut fd).unwrap();
        for (j, entry) in batch.into_entries().into_iter().enumerate() {
            let (index, op) = entry.into_index_op();
            let ref_index = (i * 1000 + j) as u64 + 1;
            assert_eq!(index, ref_index);
            let (k, v) = ((10 * i + j) as i64, (20 + i) as i64);
            let ref_op = wal::Op::<i64, i64>::new_set(k, v);
            assert_eq!(op, ref_op);
        }
    }
}
