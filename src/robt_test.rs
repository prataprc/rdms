use super::*;

#[test]
fn test_stats() {
    let vlog_file: &ffi::OsStr = "robt-users-level-1.vlog".as_ref();

    let stats1 = Stats {
        z_blocksize: 16384,
        m_blocksize: 4096,
        v_blocksize: 65536,
        delta_ok: true,
        vlog_file: Some(vlog_file.to_os_string()),
        value_in_vlog: true,

        n_count: 1000000,
        n_deleted: 100,
        seqno: 2000000,
        key_mem: 128000000,
        diff_mem: 512000000,
        val_mem: 1024000000,
        z_bytes: 256000000,
        v_bytes: 2048000000,
        m_bytes: 256000000,
        padding: 100000000,
        n_abytes: 0,

        build_time: 10000000000000,
        epoch: 121345678998765,
    };
    let s = stats1.to_string();
    let stats2: Stats = s.parse().unwrap();
    assert!(stats1 == stats2);

    let vlog_file: &ffi::OsStr = "robt-users-level-1.vlog".as_ref();
    let cnf = Config {
        z_blocksize: 16384,
        m_blocksize: 4096,
        v_blocksize: 65536,
        delta_ok: true,
        vlog_file: Some(vlog_file.to_os_string()),
        value_in_vlog: true,
        tomb_purge: None,
        flush_queue_size: 1024,
    };
    let stats1: Stats = cnf.into();
    let s = stats1.to_string();
    let stats2: Stats = s.parse().unwrap();
    assert!(stats1 == stats2);
}

#[test]
fn test_config() {
    use std::time::SystemTime;

    let dir = std::env::temp_dir().to_str().unwrap().to_string();
    fs::remove_file(dir.clone()).ok();
    let name = "users".to_string();
    let file = Config::stitch_index_file(&dir, &name);
    fs::write(&file, [1, 2, 3, 4, 5]);

    let n: u64 = (SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % (std::u64::MAX as u128))
        .try_into()
        .unwrap();
    let len1 = ROOT_MARKER.len();
    let stats = <Stats as Default>::default().to_string();
    let len2 = (n % 65536) as usize;
    let meta_data: Vec<u8> = (0..len2).map(|x| (x % 256) as u8).collect();
    let len3 = stats.len();

    let meta_items = vec![
        MetaItem::Root(5),
        MetaItem::Metadata(meta_data.clone()),
        MetaItem::Stats(stats.clone()),
        MetaItem::Marker(ROOT_MARKER.clone()),
    ];
    let n = write_meta_items(file, meta_items).unwrap();
    let ref_n = Config::compute_root_block(32 + len1 + len2 + len3);
    assert_eq!(n, ref_n as u64);

    let iter = read_meta_items(&dir, &name).unwrap().into_iter();
    for (i, item) in iter.enumerate() {
        match (i, item) {
            (0, MetaItem::Root(value)) => assert_eq!(value, 5),
            (1, MetaItem::Metadata(value)) => assert_eq!(value, meta_data),
            (2, MetaItem::Stats(value)) => assert_eq!(value, stats),
            (3, MetaItem::Marker(v)) => assert_eq!(v, ROOT_MARKER.clone()),
            (i, _) => panic!("at {}, failure", i),
        }
    }
}
