use rand::prelude::random;

use super::*;
use crate::core;
use crate::vlog;

#[test]
fn test_zblock1() {
    // value_in_vlog = false, delta_ok = false
    let vpos = 0x786;
    let mut config: Config = Default::default();
    config.value_in_vlog = false;
    config.delta_ok = false;
    let mut zb = ZBlock::new_encode(vpos, config.clone());
    assert_eq!(zb.has_first_key(), false);

    let mut entries = gen_entries(0x100, 100000);
    let mut stats: Stats = Default::default();
    let (mut size, mut val_mem) = (0, 0);
    for (i, entry) in entries.iter_mut().enumerate() {
        if entry.is_deleted() {
            size += 28;
        } else {
            size += 32;
        }
        match zb.insert(entry, &mut stats) {
            Ok(n) => assert_eq!(n, (i as u64) + 1),
            Err(Error::__ZBlockOverflow(n)) => {
                assert_eq!(n, size + 4 + (i + 1) * 4);
                entries.truncate(i);
                size -= 4;
                break;
            }
            _ => unreachable!(),
        }
        assert_eq!(zb.has_first_key(), true);
        if !entry.is_deleted() {
            val_mem += 4;
        }
    }
    assert_eq!(entries[0].as_key(), zb.as_first_key());
    assert_eq!(stats.val_mem, val_mem);
    assert_eq!(stats.key_mem, entries.len() * 4);
    assert_eq!(stats.diff_mem, 0);
    assert_eq!(stats.padding, 0);
    assert_eq!(stats.m_bytes, 0);
    assert_eq!(stats.z_bytes, 0);
    assert_eq!(stats.v_bytes, 0);

    let (z_bytes, v_bytes) = zb.finalize(&mut stats);
    assert_eq!(z_bytes, 4096);
    assert_eq!(v_bytes, 0);
    assert_eq!(stats.val_mem, val_mem);
    assert_eq!(stats.key_mem, entries.len() * 4);
    assert_eq!(stats.diff_mem, 0);
    assert_eq!(stats.m_bytes, 0);
    assert_eq!(stats.z_bytes, 4096);
    assert_eq!(stats.v_bytes, 0);

    // flush
    let (leaf, _blob) = zb.buffer();
    let file = {
        let mut dir = std::env::temp_dir();
        dir.push("test-zblock1-leaf.dat");
        let file = dir.into_os_string();
        fs::write(&file, &leaf);
        file
    };

    let (mut fd, fpos) = (util::open_file_r(&file).unwrap(), 0);
    let zb = ZBlock::<i32, i32>::new_decode(&mut fd, fpos, &config).unwrap();
    assert_eq!(zb.len(), entries.len());

    for (i, entry) in entries.iter().enumerate() {
        let (index, e) = zb
            .find(&entry.to_key(), Bound::Unbounded, Bound::Unbounded)
            .unwrap();
        assert_eq!(index, i);
        assert_eq!(e.to_key(), entry.to_key());
        assert_eq!(e.to_native_value(), entry.to_native_value());
        assert_eq!(e.to_seqno(), entry.to_seqno());
        assert_eq!(e.to_delta_count(), 0);
    }
    let key = entries[0].to_key() - 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__ZBlockExhausted(0)) => (),
        _ => unreachable!(),
    }
    let key = entries[entries.len() - 1].to_key() + 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__ZBlockExhausted(key)) => (),
        _ => unreachable!(),
    }
}

//#[test]
//fn test_zblock2() {
//    // value_in_vlog = false, delta_ok = true
//    let vpos = 0x786;
//    let mut config: Config = Default::default();
//    config.value_in_vlog = false;
//    config.delta_ok = true;
//    let mut zb = ZBlock::new_encode(vpos, config.clone());
//    assert_eq!(zb.has_first_key(), false);
//
//    let mut entries = gen_entries(0x100, 100000);
//    let mut stats: Stats = Default::default();
//    for (i, entry) in entries.iter_mut().enumerate() {
//        match zb.insert(entry, &mut stats) {
//            Ok(n) => assert_eq!(n, (i as u64) + 1),
//            Err(Error::__ZBlockOverflow(n)) => {
//                assert_eq!(n, 4108);
//                entries.truncate(i);
//                break;
//            }
//            _ => unreachable!(),
//        }
//        assert_eq!(zb.has_first_key(), true);
//    }
//    assert_eq!(entries.len(), 113);
//    assert_eq!(entries[0].as_key(), zb.as_first_key());
//    assert_eq!(stats.val_mem, 452);
//    assert_eq!(stats.key_mem, 452);
//    assert_eq!(stats.diff_mem, 0);
//    assert_eq!(stats.padding, 0);
//    assert_eq!(stats.m_bytes, 0);
//    assert_eq!(stats.z_bytes, 0);
//    assert_eq!(stats.v_bytes, 0);
//
//    let (z_bytes, v_bytes) = zb.finalize(&mut stats);
//    assert_eq!(z_bytes, 4096);
//    assert_eq!(v_bytes, 0);
//    assert_eq!(stats.val_mem, 452);
//    assert_eq!(stats.key_mem, 452);
//    assert_eq!(stats.diff_mem, 0);
//    assert_eq!(stats.padding, 24);
//    assert_eq!(stats.m_bytes, 0);
//    assert_eq!(stats.z_bytes, 4096);
//    assert_eq!(stats.v_bytes, 0);
//
//    // flush
//    let (leaf, _blob) = zb.buffer();
//    let file = {
//        let mut dir = std::env::temp_dir();
//        dir.push("test-zblock2-leaf.dat");
//        let file = dir.into_os_string();
//        fs::write(&file, &leaf);
//        file
//    };
//
//    let (mut fd, fpos) = (util::open_file_r(&file).unwrap(), 0);
//    let zb = ZBlock::<i32, i32>::new_decode(&mut fd, fpos, &config).unwrap();
//    assert_eq!(zb.len(), entries.len());
//
//    for (i, entry) in entries.iter().enumerate() {
//        let (index, e) = zb
//            .find(&entry.to_key(), Bound::Unbounded, Bound::Unbounded)
//            .unwrap();
//        assert_eq!(index, i);
//        assert_eq!(e.to_key(), entry.to_key());
//        assert_eq!(e.to_native_value(), entry.to_native_value());
//        assert_eq!(e.to_seqno(), entry.to_seqno());
//        assert_eq!(e.to_delta_count(), entry.to_delta_count());
//    }
//    match zb.find(&0, Bound::Unbounded, Bound::Unbounded) {
//        Err(Error::__ZBlockExhausted(0)) => (),
//        _ => unreachable!(),
//    }
//    match zb.find(&114, Bound::Unbounded, Bound::Unbounded) {
//        Err(Error::__ZBlockExhausted(112)) => (),
//        _ => unreachable!(),
//    }
//}

#[test]
fn test_zblock3() {
    // value_in_vlog = true, delta_ok = false
    let vpos = 0x786;
    let mut config: Config = Default::default();
    config.value_in_vlog = true;
    config.delta_ok = false;
    let mut zb = ZBlock::new_encode(vpos, config.clone());
    assert_eq!(zb.has_first_key(), false);

    let mut entries = gen_entries(0x100, 100000);
    let mut stats: Stats = Default::default();
    let (mut size, mut val_mem) = (0, 0);
    for (i, entry) in entries.iter_mut().enumerate() {
        if entry.is_deleted() {
            size += 28;
        } else {
            size += 36;
        }
        match zb.insert(entry, &mut stats) {
            Ok(n) => assert_eq!(n, (i as u64) + 1),
            Err(Error::__ZBlockOverflow(n)) => {
                assert_eq!(n, size + 4 + (i + 1) * 4);
                entries.truncate(i);
                size -= 4;
                break;
            }
            _ => unreachable!(),
        }
        assert_eq!(zb.has_first_key(), true);
        if !entry.is_deleted() {
            val_mem += 12;
        }
    }
    assert_eq!(entries[0].as_key(), zb.as_first_key());
    assert_eq!(stats.val_mem, val_mem);
    assert_eq!(stats.key_mem, entries.len() * 4);
    assert_eq!(stats.diff_mem, 0);
    assert_eq!(stats.padding, 0);
    assert_eq!(stats.m_bytes, 0);
    assert_eq!(stats.z_bytes, 0);
    assert_eq!(stats.v_bytes, 0);

    let (z_bytes, v_bytes) = zb.finalize(&mut stats);
    assert_eq!(z_bytes, 4096);
    assert_eq!(v_bytes, val_mem as u64);
    assert_eq!(stats.val_mem, val_mem);
    assert_eq!(stats.key_mem, entries.len() * 4);
    assert_eq!(stats.diff_mem, 0);
    assert_eq!(stats.m_bytes, 0);
    assert_eq!(stats.z_bytes, 4096);
    assert_eq!(stats.v_bytes, val_mem);

    // flush
    let (leaf, blob) = zb.buffer();
    let file = {
        let mut dir = std::env::temp_dir();
        dir.push("test-zblock3-leaf.dat");
        let file = dir.into_os_string();
        fs::write(&file, &leaf);
        file
    };

    let (mut fd, fpos) = (util::open_file_r(&file).unwrap(), 0);
    let zb = ZBlock::<i32, i32>::new_decode(&mut fd, fpos, &config).unwrap();
    assert_eq!(zb.len(), entries.len());

    let mut voff = 0;
    for (i, entry) in entries.iter().enumerate() {
        let (index, e) = zb
            .find(&entry.to_key(), Bound::Unbounded, Bound::Unbounded)
            .unwrap();
        assert_eq!(index, i);
        assert_eq!(e.to_key(), entry.to_key());
        assert_eq!(e.to_native_value(), None);
        assert_eq!(e.to_seqno(), entry.to_seqno());
        assert_eq!(e.to_delta_count(), 0);
        match e.as_value() {
            core::Value::D { seqno } => assert_eq!(*seqno, entry.to_seqno()),
            core::Value::U {
                value:
                    vlog::Value::Reference {
                        fpos,
                        length,
                        seqno,
                    },
                ..
            } => {
                let s: [u8; 4] = blob[voff + 8..voff + 12].try_into().unwrap();
                assert_eq!(*seqno, entry.to_seqno());
                assert_eq!(*fpos, vpos + voff as u64);
                assert_eq!(*length, 12);
                let value = entry.to_native_value().unwrap();
                assert_eq!(i32::from_be_bytes(s), value);
                voff += 12;
            }
            _ => unreachable!(),
        }
    }
    let key = entries[0].to_key() - 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__ZBlockExhausted(key)) => (),
        _ => unreachable!(),
    }
    let key = entries[entries.len() - 1].to_key() + 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__ZBlockExhausted(key)) => (),
        _ => unreachable!(),
    }
}

#[test]
fn test_zblock4() {
    // value_in_vlog = true, delta_ok = true
}

fn gen_entries(n: usize, mut seqno: u64) -> Vec<core::Entry<i32, i32>> {
    let mut entries = vec![];
    for i in 0..n {
        let (key, val): (i32, i32) = ((i as i32) + 1, random());
        let value = Box::new(core::Value::new_upsert_value(val, seqno));
        entries.push(core::Entry::new(key, value));
        seqno += 1;
    }

    for _i in 0..(entries.len() * 3) {
        let j = (random::<i32>() as usize) % entries.len();
        let entry = &mut entries[j];
        let key = entry.to_key();
        match random::<u8>() % 3 {
            0 => {
                let v: i32 = random();
                let value = Box::new(core::Value::new_upsert_value(v, seqno));
                entry.prepend_version(core::Entry::new(key, value), false);
            }
            1 => {
                let v: i32 = random();
                let value = Box::new(core::Value::new_upsert_value(v, seqno));
                entry.prepend_version(core::Entry::new(key, value), true);
            }
            2 => {
                entry.delete(seqno);
            }
            _ => unreachable!(),
        }
        seqno += 1;
    }

    entries
}
