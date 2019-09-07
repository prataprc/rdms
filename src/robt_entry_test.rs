use super::*;
use crate::core;

#[test]
fn test_mentry() {
    // m-block pointing to m-block.
    let mut buf = vec![];
    let (fpos, key) = (0x1234567, 100_i32);
    let me = MEntry::new_m(fpos, &key);
    assert_eq!(me.encode(&mut buf).unwrap(), 20);

    let buf_ref = [
        0, 0, 0, 0, 0x00, 0x00, 0x00, 0x04, // flags, klen
        0, 0, 0, 0, 0x01, 0x23, 0x45, 0x67, // child fpos
        /*       */ 0x00, 0x00, 0x00, 0x64, // key
    ];
    assert_eq!(buf.len(), buf_ref.len());
    assert_eq!(buf, buf_ref);
    // test decode logic
    assert_eq!(MEntry::<i32>::decode_key(&buf).unwrap(), 100);
    assert_eq!(me.is_zblock(), false);
    let index = 0x987;
    match MEntry::<i32>::decode_entry(&buf, index) {
        MEntry::DecM { fpos, index } => {
            assert_eq!(fpos, 0x1234567);
            assert_eq!(index, 0x987);
        }
        _ => unreachable!(),
    }

    // m-block pointing to z-block.
    let mut buf = vec![];
    let (fpos, key) = (0x1234567, 100_i32);
    let me = MEntry::new_z(fpos, &key);
    assert_eq!(me.encode(&mut buf).unwrap(), 20);

    // buf.iter().for_each(|x| print!("{:x} ", x));
    let buf_ref = [
        0x10, 0, 0, 0, 0x00, 0x00, 0x00, 0x04, // flags, klen
        0x00, 0, 0, 0, 0x01, 0x23, 0x45, 0x67, // child fpos
        /*          */ 0x00, 0x00, 0x00, 0x64, // key
    ];
    assert_eq!(buf.len(), buf_ref.len());
    assert_eq!(buf, buf_ref);
    // test decode logic
    assert_eq!(MEntry::<i32>::decode_key(&buf).unwrap(), 100);
    assert_eq!(me.is_zblock(), true);
    let index = 0x987;
    match MEntry::<i32>::decode_entry(&buf, index) {
        MEntry::DecZ { fpos, index } => {
            assert_eq!(fpos, 0x1234567);
            assert_eq!(index, 0x987);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_disk_delta() {
    // test encode
    let d = 20.diff(&30);
    let delta = core::Delta::<i32>::new_upsert(vlog::Delta::new_native(d), 101);
    let (mut leaf, mut blob) = (vec![], vec![]);
    let n = DiskDelta::encode(&delta, &mut leaf, &mut blob).unwrap();
    assert_eq!(n, 12);

    let leaf_ref = vec![
        0x10, 0, 0, 0, 0, 0, 0, 0x0c, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0x65, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0x00, // fpos
    ];
    let blob_ref = vec![
        0, 0, 0, 0, 0, 0, 0, 0x04, // length
        /*       */ 0, 0, 0, 0x1e, // payload
    ];

    assert_eq!(leaf.len(), leaf_ref.len());
    assert_eq!(blob.len(), blob_ref.len());
    assert_eq!(leaf, leaf_ref);
    assert_eq!(blob, blob_ref);

    // test re-encode
    DiskDelta::<i32>::re_encode_fpos(&mut leaf, 0x1234);
    let leaf_ref = vec![
        0x10, 0, 0, 0, 0, 0, 0x00, 0x0c, // dlen
        0x00, 0, 0, 0, 0, 0, 0x00, 0x65, // seqno
        0x00, 0, 0, 0, 0, 0, 0x12, 0x34, // fpos
    ];
    assert_eq!(leaf, leaf_ref);

    // test decode
    let delta = DiskDelta::<i32>::decode_delta(&leaf).unwrap();
    match delta.into_upserted() {
        Some((
            vlog::Delta::Reference {
                fpos,
                length,
                seqno,
            },
            seqno1,
        )) => {
            assert_eq!(fpos, 0x1234);
            assert_eq!(length, 0xc);
            assert_eq!(seqno, 0x65);
            assert_eq!(seqno, seqno1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_zentry_header() {
    let test_cases = vec![
        (
            0x23_usize,
            1_usize,
            0xabcd_usize,
            false,
            false,
            0x34567812345678_u64,
            [
                0x00, 0x00, 0x00, 0x23, 0x00, 0x00, 0x00, 0x01, // n_deltas + klen
                0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0xab, 0xcd, // vlen
                0x00, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, // seqno
            ],
        ),
        (
            0x23_usize,
            10_usize,
            0xabcd_usize,
            false,
            true,
            0x12347812345678_u64,
            [
                0x00, 0x00, 0x00, 0x23, 0x00, 0x00, 0x00, 0x0a, // n_deltas + klen
                0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0xab, 0xcd, // vlen
                0x00, 0x12, 0x34, 0x78, 0x12, 0x34, 0x56, 0x78, // seqno
            ],
        ),
        (
            0x23_usize,
            10_usize,
            0xabcd_usize,
            true,
            false,
            0x1234567812345678_u64,
            [
                0x00, 0x00, 0x00, 0x23, 0x00, 0x00, 0x00, 0x0a, // n_deltas + klen
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xab, 0xcd, // vlen
                0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, // seqno
            ],
        ),
        (
            0x23_usize,
            10_usize,
            0xabcd_usize,
            true,
            true,
            0x1234567812345678_u64,
            [
                0x00, 0x00, 0x00, 0x23, 0x00, 0x00, 0x00, 0x0a, // n_deltas + klen
                0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0xab, 0xcd, // vlen
                0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, // seqno
            ],
        ),
    ];

    let mut leaf = vec![];
    for (k, d, v, del, vlog, seqno, ref_out) in test_cases.into_iter() {
        leaf.resize(24, 0);
        ZEntry::<i32, i32>::encode_header(k, d, v, del, vlog, seqno, &mut leaf);
        assert_eq!(leaf, ref_out);
        leaf.truncate(0);
    }
}

#[test]
fn test_zentry_key() {
    let key = 100_i32;
    let mut buf = vec![];
    assert_eq!(ZEntry::<i32, i32>::encode_key(&key, &mut buf).unwrap(), 4);
}

#[test]
fn test_zentry_value() {
    let value = Box::new(core::Value::new_upsert_value(10000, 10));
    let entry = core::Entry::new(100, value);
    let mut buf = vec![];
    assert_eq!(
        ZEntry::<i32, i32>::encode_value_vlog(&entry, &mut buf).unwrap(),
        (12, false, 10)
    );

    let value = Box::new(core::Value::new_delete(11));
    let entry = core::Entry::new(100, value);
    let mut buf = vec![];
    assert_eq!(
        ZEntry::<i32, i32>::encode_value_vlog(&entry, &mut buf).unwrap(),
        (0, true, 11)
    );
}

#[test]
fn test_zentry_deltas() {
    let value = Box::new(core::Value::new_upsert_value(10000, 10));
    let mut entry = core::Entry::new(100, value);

    let value = Box::new(core::Value::new_upsert_value(20000, 11));
    entry.prepend_version(core::Entry::new(100, value), true);

    entry.delete(12);

    let value = Box::new(core::Value::new_upsert_value(30000, 13));
    entry.prepend_version(core::Entry::new(100, value), true);

    let (mut leaf, mut blob) = (vec![], vec![]);
    ZEntry::encode_deltas(&entry, &mut leaf, &mut blob).unwrap();

    assert_eq!(leaf.len(), 72);
    assert_eq!(blob.len(), 24);

    let leaf_ref = vec![
        // delete seqno 12
        0, 0, 0, 0, 0, 0, 0, 0x0, // dlen
        0, 0, 0, 0, 0, 0, 0, 0xc, // seqno
        0, 0, 0, 0, 0, 0, 0, 0x0, // fpos
        // upsert seqno 11
        0x10, 0, 0, 0, 0, 0, 0, 0xc, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0xb, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0x0, // fpos
        // upsert seqno 10
        0x10, 0, 0, 0, 0, 0, 0, 0xc, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0xa, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0xc, // fpos
    ];
    let blob_ref = vec![
        0, 0, 0x00, 0x00, 0, 0, 0x00, 0x04, // length
        /*             */ 0, 0, 0x4e, 0x20, // payload
        0, 0, 0x00, 0x00, 0, 0, 0x00, 0x04, // length
        /*             */ 0, 0, 0x27, 0x10, // payload
    ];

    assert_eq!(leaf.len(), leaf_ref.len());
    assert_eq!(blob.len(), blob_ref.len());
    assert_eq!(leaf, leaf_ref);
    assert_eq!(blob, blob_ref);
}

#[test]
fn test_zentry_l() {
    let value = Box::new(core::Value::new_upsert_value(10000, 10));
    let mut entry = core::Entry::new(100, value);

    let value = Box::new(core::Value::new_upsert_value(20000, 11));
    entry.prepend_version(core::Entry::new(100, value), true);

    entry.delete(12);

    let value = Box::new(core::Value::new_upsert_value(30000, 13));
    entry.prepend_version(core::Entry::new(100, value), true);

    let mut leaf = vec![];
    let ze = ZEntry::<i32, i32>::encode_l(&entry, &mut leaf).unwrap();
    let (k, v, d) = ze.to_kvd_stats();
    assert_eq!((k, v, d), (4, 4, 0));

    let leaf_ref = vec![
        0x00, 0, 0, 4, 0, 0, 0x00, 0x00, // klen + n_deltas
        0x10, 0, 0, 0, 0, 0, 0x00, 0x04, // vlen
        0x00, 0, 0, 0, 0, 0, 0x00, 0x0d, // seqno
        /*          */ 0, 0, 0x00, 0x64, // key
        /*          */ 0, 0, 0x75, 0x30, // value
    ];
    assert_eq!(leaf.len(), leaf_ref.len());
    assert_eq!(leaf, leaf_ref);

    // decode_key
    assert_eq!(ZEntry::<i32, i32>::decode_key(&leaf).unwrap(), 100);
    // decode_entry
    let entry_out = ZEntry::<i32, i32>::decode_entry(&leaf).unwrap();
    assert_eq!(entry_out.to_key(), 100);
    assert_eq!(entry_out.to_native_value(), Some(30000));
}

#[test]
fn test_zentry_ld() {
    let value = Box::new(core::Value::new_upsert_value(10000, 10));
    let mut entry = core::Entry::new(100, value);

    let value = Box::new(core::Value::new_upsert_value(20000, 11));
    entry.prepend_version(core::Entry::new(100, value), true);

    entry.delete(12);

    let value = Box::new(core::Value::new_upsert_value(30000, 13));
    entry.prepend_version(core::Entry::new(100, value), true);

    let (mut leaf, mut blob): (Vec<u8>, Vec<u8>) = (vec![], vec![]);
    let ze = ZEntry::<i32, i32>::encode_ld(
        &entry, &mut leaf, &mut blob, // arguments
    )
    .unwrap();
    let (k, v, d) = ze.to_kvd_stats();
    assert_eq!((k, v, d), (4, 4, 24));

    // leaf.iter().for_each(|x| print!("{:x} ", x));
    let leaf_ref = vec![
        0x00, 0, 0, 4, 0, 0, 0x00, 0x03, // klen + n_deltas
        0x10, 0, 0, 0, 0, 0, 0x00, 0x04, // vlen
        0x00, 0, 0, 0, 0, 0, 0x00, 0x0d, // seqno
        /*          */ 0, 0, 0x00, 0x64, // key
        /*          */ 0, 0, 0x75, 0x30, // value
        // delete seqno 12
        0, 0, 0, 0, 0, 0, 0, 0x0, // dlen
        0, 0, 0, 0, 0, 0, 0, 0xc, // seqno
        0, 0, 0, 0, 0, 0, 0, 0x0, // fpos
        // upsert seqno 11
        0x10, 0, 0, 0, 0, 0, 0, 0xc, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0xb, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0x0, // fpos
        // upsert seqno 10
        0x10, 0, 0, 0, 0, 0, 0, 0xc, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0xa, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0xc, // fpos
    ];
    assert_eq!(leaf.len(), leaf_ref.len());
    assert_eq!(leaf, leaf_ref);

    let blob_ref = vec![
        0, 0, 0x00, 0x00, 0, 0, 0x00, 0x04, // length
        /*             */ 0, 0, 0x4e, 0x20, // payload
        0, 0, 0x00, 0x00, 0, 0, 0x00, 0x04, // length
        /*             */ 0, 0, 0x27, 0x10, // payload
    ];
    assert_eq!(blob.len(), blob_ref.len());
    assert_eq!(blob, blob_ref);

    // decode_key
    assert_eq!(ZEntry::<i32, i32>::decode_key(&leaf).unwrap(), 100);
    // decode_entry
    let entry_out = ZEntry::<i32, i32>::decode_entry(&leaf).unwrap();
    assert_eq!(entry_out.to_key(), 100);
    assert_eq!(entry_out.to_native_value(), Some(30000));
    for (i, entry) in entry_out.versions().enumerate() {
        match (i, entry) {
            (0, entry) => {
                assert_eq!(entry.to_key(), 100);
                assert_eq!(entry.to_seqno(), 13);
                assert_eq!(entry.to_seqno_state(), (true, 13));
                assert_eq!(entry.to_native_value(), Some(30000));
            }
            (1, entry) => {
                assert_eq!(entry.to_key(), 100);
                assert_eq!(entry.to_seqno(), 12);
                assert_eq!(entry.to_seqno_state(), (false, 12));
                assert_eq!(entry.is_deleted(), true);
            }
            (2, entry) => {
                assert_eq!(entry.to_key(), 100);
                assert_eq!(entry.to_seqno(), 11);
                assert_eq!(entry.to_seqno_state(), (true, 11));
                assert_eq!(entry.to_native_value(), Some(20000));
            }
            (3, entry) => {
                assert_eq!(entry.to_key(), 100);
                assert_eq!(entry.to_seqno(), 10);
                assert_eq!(entry.to_seqno_state(), (true, 10));
                assert_eq!(entry.to_native_value(), Some(10000));
            }
            _ => unreachable!(),
        }
    }

    // re-encode fpos
    ze.re_encode_fpos(&mut leaf, 100);
    let leaf_ref = vec![
        0x00, 0, 0, 4, 0, 0, 0x00, 0x03, // klen + n_deltas
        0x10, 0, 0, 0, 0, 0, 0x00, 0x04, // vlen
        0x00, 0, 0, 0, 0, 0, 0x00, 0x0d, // seqno
        /*          */ 0, 0, 0x00, 0x64, // key
        /*          */ 0, 0, 0x75, 0x30, // value
        // delete seqno 12
        0, 0, 0, 0, 0, 0, 0, 0x00, // dlen
        0, 0, 0, 0, 0, 0, 0, 0x0c, // seqno
        0, 0, 0, 0, 0, 0, 0, 0x00, // fpos
        // upsert seqno 11
        0x10, 0, 0, 0, 0, 0, 0, 0x0c, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0x0b, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0x64, // fpos
        // upsert seqno 10
        0x10, 0, 0, 0, 0, 0, 0, 0x0c, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0x0a, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0x70, // fpos
    ];
    assert_eq!(leaf.len(), leaf_ref.len());
    assert_eq!(leaf, leaf_ref);
}

// leaf.iter().for_each(|x| print!("{:x} ", x));

#[test]
fn test_zentry_lv() {
    let value = Box::new(core::Value::new_upsert_value(10000, 10));
    let mut entry = core::Entry::new(100, value);

    let value = Box::new(core::Value::new_upsert_value(20000, 11));
    entry.prepend_version(core::Entry::new(100, value), true);

    entry.delete(12);

    let value = Box::new(core::Value::new_upsert_value(30000, 13));
    entry.prepend_version(core::Entry::new(100, value), true);

    let (mut leaf, mut blob): (Vec<u8>, Vec<u8>) = (vec![], vec![]);
    let ze = ZEntry::<i32, i32>::encode_lv(
        &entry, &mut leaf, &mut blob, // arguments
    )
    .unwrap();
    let (k, v, d) = ze.to_kvd_stats();
    assert_eq!((4, 12, 0), (k, v, d));

    let leaf_ref = vec![
        0x00, 0, 0, 4, 0, 0, 0x00, 0x00, // klen + n_deltas
        0x30, 0, 0, 0, 0, 0, 0x00, 0x0c, // vlen
        0x00, 0, 0, 0, 0, 0, 0x00, 0x0d, // seqno
        /*          */ 0, 0, 0x00, 0x64, // key
        0x00, 0, 0, 0, 0, 0, 0x00, 0x00, // value-fpos
    ];
    assert_eq!(leaf.len(), leaf_ref.len());
    assert_eq!(leaf, leaf_ref);

    // blob.iter().for_each(|x| print!("{:x} ", x));
    let blob_ref = vec![
        0x10, 0, 0, 0x00, 0, 0, 0x00, 0x04, // length
        /*             */ 0, 0, 0x75, 0x30, // payload
    ];
    assert_eq!(blob.len(), blob_ref.len());
    assert_eq!(blob, blob_ref);

    // decode_key
    assert_eq!(ZEntry::<i32, i32>::decode_key(&leaf).unwrap(), 100);
    // decode_entry
    let entry_out = ZEntry::<i32, i32>::decode_entry(&leaf).unwrap();
    assert_eq!(entry_out.to_key(), 100);
    match entry_out.as_value() {
        core::Value::U {
            value:
                vlog::Value::Reference {
                    fpos,
                    length,
                    seqno,
                },
            seqno: seqno1,
        } => {
            assert_eq!(*fpos, 0);
            assert_eq!(*length, 12);
            assert_eq!(*seqno, 13);
            assert_eq!(*seqno1, 13);
        }
        _ => unreachable!(),
    }

    // re-encode fpos
    ze.re_encode_fpos(&mut leaf, 200);
    let leaf_ref = vec![
        0x00, 0, 0, 4, 0, 0, 0x00, 0x00, // klen + n_deltas
        0x30, 0, 0, 0, 0, 0, 0x00, 0x0c, // vlen
        0x00, 0, 0, 0, 0, 0, 0x00, 0x0d, // seqno
        /*          */ 0, 0, 0x00, 0x64, // key
        0x00, 0, 0, 0, 0, 0, 0x00, 0xc8, // value-fpos
    ];
    assert_eq!(leaf.len(), leaf_ref.len());
    assert_eq!(leaf, leaf_ref);
}

#[test]
fn test_zentry_lvd() {
    let value = Box::new(core::Value::new_upsert_value(10000, 10));
    let mut entry = core::Entry::new(100, value);

    let value = Box::new(core::Value::new_upsert_value(20000, 11));
    entry.prepend_version(core::Entry::new(100, value), true);

    entry.delete(12);

    let value = Box::new(core::Value::new_upsert_value(30000, 13));
    entry.prepend_version(core::Entry::new(100, value), true);

    let (mut leaf, mut blob): (Vec<u8>, Vec<u8>) = (vec![], vec![]);
    let ze = ZEntry::<i32, i32>::encode_lvd(
        &entry, &mut leaf, &mut blob, // arguments
    )
    .unwrap();
    let (k, v, d) = ze.to_kvd_stats();
    assert_eq!((k, v, d), (4, 12, 24));

    let leaf_ref = vec![
        0x00, 0, 0, 4, 0, 0, 0x00, 0x03, // klen + n_deltas
        0x30, 0, 0, 0, 0, 0, 0x00, 0x0c, // vlen
        0x00, 0, 0, 0, 0, 0, 0x00, 0x0d, // seqno
        /*          */ 0, 0, 0x00, 0x64, // key
        0x00, 0, 0, 0, 0, 0, 0x00, 0x00, // value-fpos
        // delete seqno 12
        0, 0, 0, 0, 0, 0, 0, 0x0, // dlen
        0, 0, 0, 0, 0, 0, 0, 0xc, // seqno
        0, 0, 0, 0, 0, 0, 0, 0x0, // fpos
        // upsert seqno 11
        0x10, 0, 0, 0, 0, 0, 0, 0xc, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0xb, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0xc, // fpos
        // upsert seqno 10
        0x10, 0, 0, 0, 0, 0, 0, 0x0c, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0x0a, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0x18, // fpos
    ];
    assert_eq!(leaf.len(), leaf_ref.len());
    assert_eq!(leaf, leaf_ref);

    // blob.iter().for_each(|x| print!("{:x} ", x));
    let blob_ref = vec![
        0x10, 0, 0, 0x00, 0, 0, 0x00, 0x04, // length
        /*             */ 0, 0, 0x75, 0x30, // payload
        0, 0, 0x00, 0x00, 0, 0, 0x00, 0x04, // length
        /*             */ 0, 0, 0x4e, 0x20, // payload
        0, 0, 0x00, 0x00, 0, 0, 0x00, 0x04, // length
        /*             */ 0, 0, 0x27, 0x10, // payload
    ];
    assert_eq!(blob.len(), blob_ref.len());
    assert_eq!(blob, blob_ref);

    // decode_key
    assert_eq!(ZEntry::<i32, i32>::decode_key(&leaf).unwrap(), 100);
    // decode_entry
    let entry_out = ZEntry::<i32, i32>::decode_entry(&leaf).unwrap();
    assert_eq!(entry_out.to_key(), 100);
    match entry_out.as_value() {
        core::Value::U {
            value:
                vlog::Value::Reference {
                    fpos,
                    length,
                    seqno,
                },
            seqno: seqno1,
        } => {
            assert_eq!(*fpos, 0);
            assert_eq!(*length, 12);
            assert_eq!(*seqno, 13);
            assert_eq!(*seqno1, 13);
        }
        _ => unreachable!(),
    }
    for (i, entry) in entry_out.versions().enumerate() {
        match (i, entry) {
            (0, entry) => {
                assert_eq!(entry.to_key(), 100);
                assert_eq!(entry.to_seqno(), 13);
                assert_eq!(entry.to_seqno_state(), (true, 13));
                assert_eq!(entry.to_native_value(), Some(30000));
            }
            (1, entry) => {
                assert_eq!(entry.to_key(), 100);
                assert_eq!(entry.to_seqno(), 12);
                assert_eq!(entry.to_seqno_state(), (false, 12));
                assert_eq!(entry.is_deleted(), true);
            }
            (2, entry) => {
                assert_eq!(entry.to_key(), 100);
                assert_eq!(entry.to_seqno(), 11);
                assert_eq!(entry.to_seqno_state(), (true, 11));
                assert_eq!(entry.to_native_value(), Some(20000));
            }
            (3, entry) => {
                assert_eq!(entry.to_key(), 100);
                assert_eq!(entry.to_seqno(), 10);
                assert_eq!(entry.to_seqno_state(), (true, 10));
                assert_eq!(entry.to_native_value(), Some(10000));
            }
            _ => unreachable!(),
        }
    }

    // re-encode fpos
    ze.re_encode_fpos(&mut leaf, 200);
    let leaf_ref = vec![
        0x00, 0, 0, 4, 0, 0, 0x00, 0x03, // klen + n_deltas
        0x30, 0, 0, 0, 0, 0, 0x00, 0x0c, // vlen
        0x00, 0, 0, 0, 0, 0, 0x00, 0x0d, // seqno
        /*          */ 0, 0, 0x00, 0x64, // key
        0x00, 0, 0, 0, 0, 0, 0x00, 0xc8, // value-fpos
        // delete seqno 12
        0, 0, 0, 0, 0, 0, 0, 0x00, // dlen
        0, 0, 0, 0, 0, 0, 0, 0x0c, // seqno
        0, 0, 0, 0, 0, 0, 0, 0x00, // fpos
        // upsert seqno 11
        0x10, 0, 0, 0, 0, 0, 0, 0x0c, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0x0b, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0xd4, // fpos
        // upsert seqno 10
        0x10, 0, 0, 0, 0, 0, 0, 0x0c, // dlen
        0x00, 0, 0, 0, 0, 0, 0, 0x0a, // seqno
        0x00, 0, 0, 0, 0, 0, 0, 0xe0, // fpos
    ];
    assert_eq!(leaf.len(), leaf_ref.len());
    assert_eq!(leaf, leaf_ref);
}
