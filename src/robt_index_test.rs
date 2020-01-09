use rand::prelude::random;
use std::fs;

use super::*;
use crate::{core, util, vlog};

#[test]
fn test_mblock_m() {
    let config: Config = Default::default();
    let mut mb = MBlock::<i32, i32>::new_encode(config.clone());
    assert_eq!(mb.has_first_key(), false);

    let mut stats: Stats = Default::default();
    let mut keys = vec![];
    for i in 0..100000 {
        let (key, fpos) = ((i + 1) * 64, (i * 4096) as u64);
        match mb.insertm(&key, fpos) {
            Ok(n) => {
                keys.push((key, fpos));
                assert_eq!(n, (i as u64) + 1);
            }
            Err(Error::__MBlockOverflow(_n)) => {
                break;
            }
            _ => unreachable!(),
        }
        assert_eq!(mb.has_first_key(), true);
    }
    assert_eq!(keys[0].0, *mb.as_first_key().unwrap());

    let m_bytes = mb.finalize(&mut stats).unwrap();
    assert_eq!(m_bytes, 4096);
    assert_eq!(stats.val_mem, 0);
    assert_eq!(stats.key_mem, 0);
    assert_eq!(stats.diff_mem, 0);
    assert_eq!(stats.m_bytes, 4096);
    assert_eq!(stats.z_bytes, 0);
    assert_eq!(stats.v_bytes, 0);

    // flush
    let mblock = mb.buffer();
    let file = {
        let mut dir = std::env::temp_dir();
        dir.push("test-mblock-m-mblock.dat");
        let file = dir.into_os_string();
        fs::write(&file, &mblock).unwrap();
        file
    };

    let mb = {
        let (mut fd, fpos) = (util::open_file_r(&file).unwrap(), 0);
        MBlock::<i32, i32>::new_decode(
            util::read_buffer(&mut fd, fpos, config.m_blocksize as u64, "reading mblock").unwrap(),
        )
        .unwrap()
    };
    assert_eq!(mb.len(), keys.len());

    for (i, entry) in keys.iter().enumerate() {
        let me = mb.to_entry(i).unwrap();
        match me {
            MEntry::DecM { fpos, index } => {
                assert_eq!(index, i);
                assert_eq!(fpos, (i * 4096) as u64);
            }
            _ => unreachable!(),
        }

        assert_eq!(mb.to_key(i).unwrap(), entry.0);

        let r = random::<u8>() % 3;
        let key = match r {
            0 => entry.0 - 1,
            1 => entry.0,
            2 => entry.0 + 1,
            _ => unreachable!(),
        };
        match mb.get(&key, Bound::Unbounded, Bound::Unbounded) {
            Ok(MEntry::DecM { fpos, index }) if r == 1 || r == 2 => {
                assert_eq!(fpos, (i * 4096) as u64);
                assert_eq!(index, i);
            }
            Ok(MEntry::DecM { fpos, index }) if i > 0 => {
                assert_eq!(fpos, ((i - 1) * 4096) as u64);
                assert_eq!(index, i - 1);
            }
            Ok(MEntry::DecM { index, .. }) => panic!("why ok {}", index),
            Err(Error::__LessThan) if i == 0 => (),
            Err(Error::__MBlockExhausted(_n)) if i == keys.len() => (),
            Err(err) => panic!("unexpected err {:?}", err),
            _ => unreachable!(),
        }

        let me = mb.find(&key, Bound::Unbounded, Bound::Unbounded);
        match me {
            Ok(MEntry::DecM { fpos, index }) if r == 1 || r == 2 => {
                assert_eq!(fpos, (i * 4096) as u64);
                assert_eq!(index, i);
            }
            Ok(MEntry::DecM { fpos, index }) if i > 0 => {
                assert_eq!(fpos, ((i - 1) * 4096) as u64);
                assert_eq!(index, i - 1);
            }
            Err(Error::__LessThan) if key == (entry.0 - 1) => (),
            Err(Error::__MBlockExhausted(_n)) if key == (entry.0 + 1) => (),
            _ => unreachable!(),
        }
    }

    // test case for last() method
    let last_i = keys.len() - 1;
    let me = mb.last().unwrap();
    match me {
        MEntry::DecM { fpos, index } => {
            assert_eq!(index, last_i);
            assert_eq!(fpos, (last_i * 4096) as u64);
        }
        _ => unreachable!(),
    }

    let index = keys.len();
    match mb.to_entry(index) {
        Err(Error::__MBlockExhausted(n)) => assert_eq!(index, n),
        _ => unreachable!(),
    }
    match mb.to_key(index) {
        Err(Error::__MBlockExhausted(n)) => assert_eq!(index, n),
        _ => unreachable!(),
    }
}

#[test]
fn test_mblock_z() {
    let config: Config = Default::default();
    let mut mb = MBlock::<i32, i32>::new_encode(config.clone());
    assert_eq!(mb.has_first_key(), false);

    let mut stats: Stats = Default::default();
    let mut keys = vec![];
    for i in 0..100000 {
        let (key, fpos) = ((i + 1) * 64, (i * 4096) as u64);
        match mb.insertz(&key, fpos) {
            Ok(n) => {
                keys.push((key, fpos));
                assert_eq!(n, (i as u64) + 1);
            }
            Err(Error::__MBlockOverflow(_n)) => {
                break;
            }
            _ => unreachable!(),
        }
        assert_eq!(mb.has_first_key(), true);
    }
    assert_eq!(keys[0].0, *mb.as_first_key().unwrap());

    let m_bytes = mb.finalize(&mut stats).unwrap();
    assert_eq!(m_bytes, 4096);
    assert_eq!(stats.val_mem, 0);
    assert_eq!(stats.key_mem, 0);
    assert_eq!(stats.diff_mem, 0);
    assert_eq!(stats.m_bytes, 4096);
    assert_eq!(stats.z_bytes, 0);
    assert_eq!(stats.v_bytes, 0);

    // flush
    let mblock = mb.buffer();
    let file = {
        let mut dir = std::env::temp_dir();
        dir.push("test-mblock-z-mblock.dat");
        let file = dir.into_os_string();
        fs::write(&file, &mblock).unwrap();
        file
    };

    let mb = {
        let (mut fd, fpos) = (util::open_file_r(&file).unwrap(), 0);
        MBlock::<i32, i32>::new_decode(
            util::read_buffer(&mut fd, fpos, config.m_blocksize as u64, "reading mblock").unwrap(),
        )
        .unwrap()
    };
    assert_eq!(mb.len(), keys.len());

    for (i, entry) in keys.iter().enumerate() {
        let me = mb.to_entry(i).unwrap();
        match me {
            MEntry::DecZ { fpos, index } => {
                assert_eq!(index, i);
                assert_eq!(fpos, (i * 4096) as u64);
            }
            _ => unreachable!(),
        }
        assert_eq!(mb.to_key(i).unwrap(), entry.0);

        let r = random::<u8>() % 3;
        let key = match r {
            0 => entry.0 - 1,
            1 => entry.0,
            2 => entry.0 + 1,
            _ => unreachable!(),
        };
        match mb.get(&key, Bound::Unbounded, Bound::Unbounded) {
            Ok(MEntry::DecZ { fpos, index }) if r == 1 || r == 2 => {
                assert_eq!(fpos, (i * 4096) as u64);
                assert_eq!(index, i);
            }
            Ok(MEntry::DecZ { fpos, index }) if i > 0 => {
                assert_eq!(fpos, ((i - 1) * 4096) as u64);
                assert_eq!(index, i - 1);
            }
            Ok(MEntry::DecZ { index, .. }) => panic!("why ok {}", index),
            Err(Error::__LessThan) if i == 0 => (),
            Err(err) => panic!("unexpected err {:?}", err),
            _ => unreachable!(),
        }

        let me = mb.find(&key, Bound::Unbounded, Bound::Unbounded);
        match me {
            Ok(MEntry::DecZ { fpos, index }) if r == 1 || r == 2 => {
                assert_eq!(fpos, (i * 4096) as u64);
                assert_eq!(index, i);
            }
            Ok(MEntry::DecZ { fpos, index }) if i > 0 => {
                assert_eq!(fpos, ((i - 1) * 4096) as u64);
                assert_eq!(index, i - 1);
            }
            Ok(MEntry::DecZ { index, .. }) => panic!("why ok {}", index),
            Err(Error::__LessThan) if i == 0 => (),
            _ => unreachable!(),
        }
    }

    let index = keys.len();
    match mb.to_entry(index) {
        Err(Error::__MBlockExhausted(n)) => assert_eq!(index, n),
        _ => unreachable!(),
    }
    match mb.to_key(index) {
        Err(Error::__MBlockExhausted(n)) => assert_eq!(index, n),
        _ => unreachable!(),
    }
}

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
    let mut val_mem = 0;
    for (i, entry) in entries.iter_mut().enumerate() {
        match zb.insert(entry, &mut stats) {
            Ok(n) => assert_eq!(n, (i as u64) + 1),
            Err(Error::__ZBlockOverflow(_n)) => {
                entries.truncate(i);
                break;
            }
            _ => unreachable!(),
        }
        assert_eq!(zb.has_first_key(), true);
        if !entry.is_deleted() {
            val_mem += 4;
        }
    }
    assert_eq!(entries[0].as_key(), zb.as_first_key().unwrap());
    assert_eq!(stats.val_mem, val_mem);
    assert_eq!(stats.key_mem, entries.len() * 4);
    assert_eq!(stats.diff_mem, 0);
    assert_eq!(stats.padding, 0);
    assert_eq!(stats.m_bytes, 0);
    assert_eq!(stats.z_bytes, 0);
    assert_eq!(stats.v_bytes, 0);

    let (z_bytes, v_bytes) = zb.finalize(&mut stats).unwrap();
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
        fs::write(&file, &leaf).unwrap();
        file
    };

    let zbs = config.z_blocksize as u64;
    let zb = {
        let (mut fd, fpos) = (util::open_file_r(&file).unwrap(), 0);
        ZBlock::<i32, i32>::new_decode(
            util::read_buffer(&mut fd, fpos, zbs, "reading zblock").unwrap(),
        )
        .unwrap()
    };
    assert_eq!(zb.len(), entries.len());

    let mut last_entry: Option<core::Entry<i32, i32>> = None;
    let mut last_index: Option<usize> = None;
    for (i, entry) in entries.iter().enumerate() {
        let (index, e) = zb
            .find(&entry.to_key(), Bound::Unbounded, Bound::Unbounded)
            .unwrap();
        assert_eq!(index, i);
        assert_eq!(e.to_key(), entry.to_key());
        assert_eq!(e.to_native_value(), entry.to_native_value());
        assert_eq!(e.to_seqno(), entry.to_seqno());
        assert_eq!(e.to_delta_count(), 0);

        last_entry = Some(e);
        last_index = Some(i);
    }

    let (last_entry, last_index) = (last_entry.unwrap(), last_index.unwrap());
    let (index, e) = zb.last().unwrap();
    assert_eq!(index, last_index);
    assert_eq!(e.to_key(), last_entry.to_key());
    assert_eq!(e.to_native_value(), last_entry.to_native_value());
    assert_eq!(e.to_seqno(), last_entry.to_seqno());

    let key = entries[0].to_key() - 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__LessThan) => (),
        _ => unreachable!(),
    }
    let key = entries[entries.len() - 1].to_key() + 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__ZBlockExhausted(k)) => assert_eq!(k as i32, key - 2),
        _ => unreachable!(),
    }
}

#[test]
fn test_zblock2() {
    // value_in_vlog = false, delta_ok = true
    let vpos = 0x786;
    let mut config: Config = Default::default();
    config.value_in_vlog = false;
    config.delta_ok = true;
    let mut zb = ZBlock::new_encode(vpos, config.clone());
    assert_eq!(zb.has_first_key(), false);

    let mut entries = gen_entries(0x100, 100000);
    let mut stats: Stats = Default::default();
    let (mut val_mem, mut diff_mem) = (0, 0);
    for (i, entry) in entries.iter_mut().enumerate() {
        match zb.insert(entry, &mut stats) {
            Ok(n) => assert_eq!(n, (i as u64) + 1),
            Err(Error::__ZBlockOverflow(_n)) => {
                entries.truncate(i);
                break;
            }
            _ => unreachable!(),
        }
        assert_eq!(zb.has_first_key(), true);
        if !entry.is_deleted() {
            val_mem += 4;
        }
        let dmem: usize = entry
            .as_deltas()
            .iter()
            .filter_map(|d| if d.is_deleted() { None } else { Some(12) })
            .sum();
        diff_mem += dmem;
    }
    assert_eq!(entries[0].as_key(), zb.as_first_key().unwrap());
    assert_eq!(stats.val_mem, val_mem);
    assert_eq!(stats.key_mem, entries.len() * 4);
    assert_eq!(stats.diff_mem, diff_mem);
    assert_eq!(stats.padding, 0);
    assert_eq!(stats.m_bytes, 0);
    assert_eq!(stats.z_bytes, 0);
    assert_eq!(stats.v_bytes, 0);

    let (z_bytes, v_bytes) = zb.finalize(&mut stats).unwrap();
    assert_eq!(z_bytes, 4096);
    assert_eq!(v_bytes, diff_mem as u64);
    assert_eq!(stats.val_mem, val_mem);
    assert_eq!(stats.key_mem, entries.len() * 4);
    assert_eq!(stats.diff_mem, diff_mem);
    assert_eq!(stats.m_bytes, 0);
    assert_eq!(stats.z_bytes, 4096);
    assert_eq!(stats.v_bytes, diff_mem);

    // flush
    let (leaf, blob) = zb.buffer();
    let file = {
        let mut dir = std::env::temp_dir();
        dir.push("test-zblock2-leaf.dat");
        let file = dir.into_os_string();
        fs::write(&file, &leaf).unwrap();
        file
    };

    let zb = {
        let (mut fd, fpos) = (util::open_file_r(&file).unwrap(), 0);
        ZBlock::<i32, i32>::new_decode(
            util::read_buffer(&mut fd, fpos, config.z_blocksize as u64, "reading zblock").unwrap(),
        )
        .unwrap()
    };
    assert_eq!(zb.len(), entries.len());

    let mut doff = 0;
    for (i, entry) in entries.iter().enumerate() {
        let (index, e) = zb
            .find(&entry.to_key(), Bound::Unbounded, Bound::Unbounded)
            .unwrap();
        assert_eq!(index, i);
        assert_eq!(e.to_key(), entry.to_key());
        assert_eq!(e.to_native_value(), entry.to_native_value());
        assert_eq!(e.to_seqno(), entry.to_seqno());
        assert_eq!(e.to_delta_count(), entry.to_delta_count());
        for (d1, d2) in e.to_deltas().iter().zip(entry.to_deltas().iter()) {
            let id1: &core::InnerDelta<i32> = d1.as_ref();
            match id1 {
                core::InnerDelta::D { seqno } => {
                    assert_eq!(*seqno, d2.to_seqno());
                }
                core::InnerDelta::U {
                    delta:
                        vlog::Delta::Reference {
                            fpos,
                            length,
                            seqno,
                        },
                    ..
                } => {
                    assert_eq!(*seqno, d2.to_seqno());
                    assert_eq!(*fpos, vpos + doff as u64);
                    assert_eq!(*length, 12);

                    let s: [u8; 4] = blob[doff + 8..doff + 12].try_into().unwrap();
                    let diff = d2.to_diff().unwrap();
                    assert_eq!(i32::from_be_bytes(s), diff);
                    doff += 12;
                }
                _ => unreachable!(),
            }
        }
    }
    let key = entries[0].to_key() - 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__LessThan) => (),
        _ => unreachable!(),
    }
    let key = entries[entries.len() - 1].to_key() + 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__ZBlockExhausted(k)) => assert_eq!(k as i32, key - 2),
        _ => unreachable!(),
    }
}

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
    let mut val_mem = 0;
    for (i, entry) in entries.iter_mut().enumerate() {
        match zb.insert(entry, &mut stats) {
            Ok(n) => assert_eq!(n, (i as u64) + 1),
            Err(Error::__ZBlockOverflow(_n)) => {
                entries.truncate(i);
                break;
            }
            _ => unreachable!(),
        }
        assert_eq!(zb.has_first_key(), true);
        if !entry.is_deleted() {
            val_mem += 12;
        }
    }
    assert_eq!(entries[0].as_key(), zb.as_first_key().unwrap());
    assert_eq!(stats.val_mem, val_mem);
    assert_eq!(stats.key_mem, entries.len() * 4);
    assert_eq!(stats.diff_mem, 0);
    assert_eq!(stats.padding, 0);
    assert_eq!(stats.m_bytes, 0);
    assert_eq!(stats.z_bytes, 0);
    assert_eq!(stats.v_bytes, 0);

    let (z_bytes, v_bytes) = zb.finalize(&mut stats).unwrap();
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
        fs::write(&file, &leaf).unwrap();
        file
    };

    let zb = {
        let (mut fd, fpos) = (util::open_file_r(&file).unwrap(), 0);
        ZBlock::<i32, i32>::new_decode(
            util::read_buffer(&mut fd, fpos, config.z_blocksize as u64, "reading zblock").unwrap(),
        )
        .unwrap()
    };
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
            core::Value::U { value, .. } if value.is_reference() => {
                let (fpos, length, seqno) = value.to_reference().unwrap();
                assert_eq!(seqno, entry.to_seqno());
                assert_eq!(fpos, vpos + voff as u64);
                assert_eq!(length, 12);

                let value = entry.to_native_value().unwrap();
                let s: [u8; 4] = blob[voff + 8..voff + 12].try_into().unwrap();
                assert_eq!(i32::from_be_bytes(s), value);
                voff += 12;
            }
            _ => unreachable!(),
        }
    }
    let key = entries[0].to_key() - 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__LessThan) => (),
        _ => unreachable!(),
    }
    let key = entries[entries.len() - 1].to_key() + 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__ZBlockExhausted(k)) => assert_eq!(k as i32, key - 2),
        _ => unreachable!(),
    }
}

#[test]
fn test_zblock4() {
    // value_in_vlog = true, delta_ok = true
    let vpos = 0x786;
    let mut config: Config = Default::default();
    config.value_in_vlog = true;
    config.delta_ok = true;
    let mut zb = ZBlock::new_encode(vpos, config.clone());
    assert_eq!(zb.has_first_key(), false);

    let mut entries = gen_entries(0x100, 100000);
    let mut stats: Stats = Default::default();
    let (mut val_mem, mut diff_mem) = (0, 0);
    for (i, entry) in entries.iter_mut().enumerate() {
        match zb.insert(entry, &mut stats) {
            Ok(n) => assert_eq!(n, (i as u64) + 1),
            Err(Error::__ZBlockOverflow(_n)) => {
                entries.truncate(i);
                break;
            }
            _ => unreachable!(),
        }
        assert_eq!(zb.has_first_key(), true);
        if !entry.is_deleted() {
            val_mem += 12;
        }
        let dmem: usize = entry
            .as_deltas()
            .iter()
            .filter_map(|d| if d.is_deleted() { None } else { Some(12) })
            .sum();
        diff_mem += dmem;
    }
    assert_eq!(entries[0].as_key(), zb.as_first_key().unwrap());
    assert_eq!(stats.val_mem, val_mem);
    assert_eq!(stats.key_mem, entries.len() * 4);
    assert_eq!(stats.diff_mem, diff_mem);
    assert_eq!(stats.padding, 0);
    assert_eq!(stats.m_bytes, 0);
    assert_eq!(stats.z_bytes, 0);
    assert_eq!(stats.v_bytes, 0);

    let (z_bytes, v_bytes) = zb.finalize(&mut stats).unwrap();
    assert_eq!(z_bytes, 4096);
    assert_eq!(v_bytes, (diff_mem + val_mem) as u64);
    assert_eq!(stats.val_mem, val_mem);
    assert_eq!(stats.key_mem, entries.len() * 4);
    assert_eq!(stats.diff_mem, diff_mem);
    assert_eq!(stats.m_bytes, 0);
    assert_eq!(stats.z_bytes, 4096);
    assert_eq!(stats.v_bytes, val_mem + diff_mem);

    // flush
    let (leaf, blob) = zb.buffer();
    let file = {
        let mut dir = std::env::temp_dir();
        dir.push("test-zblock4-leaf.dat");
        let file = dir.into_os_string();
        fs::write(&file, &leaf).unwrap();
        file
    };

    let zb = {
        let (mut fd, fpos) = (util::open_file_r(&file).unwrap(), 0);
        ZBlock::<i32, i32>::new_decode(
            util::read_buffer(&mut fd, fpos, config.z_blocksize as u64, "reading zblock").unwrap(),
        )
        .unwrap()
    };
    assert_eq!(zb.len(), entries.len());

    let (mut doff, mut voff) = (0, 0);
    for (i, entry) in entries.iter().enumerate() {
        let (index, e) = zb
            .find(&entry.to_key(), Bound::Unbounded, Bound::Unbounded)
            .unwrap();
        assert_eq!(index, i);
        assert_eq!(e.to_key(), entry.to_key());
        assert_eq!(e.to_native_value(), None);
        assert_eq!(e.to_seqno(), entry.to_seqno());
        assert_eq!(e.to_delta_count(), entry.to_delta_count());
        match e.as_value() {
            core::Value::D { seqno } => assert_eq!(*seqno, entry.to_seqno()),
            core::Value::U { value, .. } if value.is_reference() => {
                let (fpos, length, seqno) = value.to_reference().unwrap();
                assert_eq!(seqno, entry.to_seqno());
                assert_eq!(fpos, vpos + voff as u64);
                assert_eq!(length, 12);

                let value = entry.to_native_value().unwrap();
                let s: [u8; 4] = blob[voff + 8..voff + 12].try_into().unwrap();
                assert_eq!(i32::from_be_bytes(s), value);
                voff += 12;
                doff += 12;
            }
            _ => unreachable!(),
        }
        for (d1, d2) in e.to_deltas().iter().zip(entry.to_deltas().iter()) {
            let id1: &core::InnerDelta<i32> = d1.as_ref();
            match id1 {
                core::InnerDelta::D { seqno } => {
                    assert_eq!(*seqno, d2.to_seqno());
                }
                core::InnerDelta::U {
                    delta:
                        vlog::Delta::Reference {
                            fpos,
                            length,
                            seqno,
                        },
                    ..
                } => {
                    assert_eq!(*seqno, d2.to_seqno());
                    assert_eq!(*fpos, vpos + doff as u64);
                    assert_eq!(*length, 12);

                    let s: [u8; 4] = blob[doff + 8..doff + 12].try_into().unwrap();
                    let diff = d2.to_diff().unwrap();
                    assert_eq!(i32::from_be_bytes(s), diff);
                    doff += 12;
                    voff += 12;
                }
                _ => unreachable!(),
            }
        }
    }
    let key = entries[0].to_key() - 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__LessThan) => (),
        _ => unreachable!(),
    }
    let key = entries[entries.len() - 1].to_key() + 1;
    match zb.find(&key, Bound::Unbounded, Bound::Unbounded) {
        Err(Error::__ZBlockExhausted(k)) => assert_eq!(k as i32, key - 2),
        _ => unreachable!(),
    }
}

fn gen_entries(n: usize, mut seqno: u64) -> Vec<core::Entry<i32, i32>> {
    let mut entries = vec![];
    for i in 0..n {
        let (key, val): (i32, i32) = ((i as i32) + 1, random());
        let value = core::Value::new_upsert_value(val, seqno);
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
                let value = core::Value::new_upsert_value(v, seqno);
                entry
                    .prepend_version(core::Entry::new(key, value), false)
                    .ok();
            }
            1 => {
                let v: i32 = random();
                let value = core::Value::new_upsert_value(v, seqno);
                entry
                    .prepend_version(core::Entry::new(key, value), true)
                    .ok();
            }
            2 => {
                entry.delete(seqno).unwrap();
            }
            _ => unreachable!(),
        }
        seqno += 1;
    }

    entries
}
