use super::*;

#[test]
fn test_robt_index_file() {
    let name = "test-index-file".to_string();
    let out = AsRef::<ffi::OsStr>::as_ref("test-index-file-robt.indx").to_os_string();

    let index_file = IndexFileName::from(name.clone());
    assert_eq!(index_file.0, out);
    assert_eq!(String::try_from(index_file.clone()).unwrap(), name);
    assert_eq!(ffi::OsString::from(index_file), out);
}

#[test]
fn test_robt_vlog_file() {
    let name = "test-vlog-file".to_string();
    let out = AsRef::<ffi::OsStr>::as_ref("test-vlog-file-robt.vlog").to_os_string();

    let vlog_file = VlogFileName::from(name);
    assert_eq!(vlog_file.0, out);
    assert_eq!(ffi::OsString::from(vlog_file), out);
}
