use std::{
    fs,
    io::{self, Read, Seek, Write},
    path::PathBuf,
};

use crate::error::Error;

use super::*;

#[test]
fn test_open_file_rw() {
    // case 1: try to create empty file.
    let dir = PathBuf::new();
    let fd = create_file_a(dir.as_os_str());
    match fd.expect_err("expected invalid-file") {
        Error::InvalidFile(_, _) => (),
        err => panic!("{:?}", err),
    }

    // case 2: try to create root dir as file.
    let mut dir = PathBuf::new();
    dir.push("/");
    let fd = create_file_a(dir.as_os_str());
    match fd.expect_err("expected invalid-file") {
        Error::InvalidFile(_, _) => (),
        err => panic!("{:?}", err),
    }

    // case 3: with valid file, reuse: false
    let mut dir = std::env::temp_dir();
    dir.push("rust.rdms.util.open_file_rw.txt");
    let file = dir.as_path();

    fs::remove_file(file).ok();

    let mut fd = create_file_a(file.as_os_str()).expect("open-write");
    assert_eq!(fd.write("hello world".as_bytes()).expect("write failed"), 11);
    fd.seek(io::SeekFrom::Start(1)).expect("seek failed");
    assert_eq!(fd.write("i world".as_bytes()).expect("write failed"), 7);

    let txt = fs::read(file).expect("read failed");
    assert_eq!(std::str::from_utf8(&txt).unwrap(), "hello worldi world");

    // case 4: with valid file, reuse: false, recreate
    let mut dir = std::env::temp_dir();
    dir.push("rust.rdms.util.open_file_rw.txt");
    let file = dir.as_path();

    let mut fd = create_file_a(file.as_os_str()).expect("open-write");
    assert_eq!(fd.write("hello world".as_bytes()).expect("write failed"), 11);
    fd.seek(io::SeekFrom::Start(1)).expect("seek failed");
    assert_eq!(fd.write("i world".as_bytes()).expect("write failed"), 7);

    let txt = fs::read(file).expect("read failed");
    assert_eq!(std::str::from_utf8(&txt).unwrap(), "hello worldi world");

    // case 5: with valid file, reuse: true, reuse file.
    let mut dir = std::env::temp_dir();
    dir.push("rust.rdms.util.open_file_rw.txt");
    let file = dir.as_path();

    let mut fd = open_file_a(file.as_os_str()).expect("open-write");
    assert_eq!(fd.write("hello world".as_bytes()).expect("write failed"), 11);
    fd.seek(io::SeekFrom::Start(1)).expect("seek failed");
    assert_eq!(fd.write("i world".as_bytes()).expect("write failed"), 7);

    let txt = fs::read(&file).expect("read failed");
    assert_eq!(
        std::str::from_utf8(&txt).unwrap(),
        "hello worldi worldhello worldi world"
    );

    // case 6: read file.
    let mut fd = open_file_r(file.as_ref()).expect("open-read");
    let mut txt = [0_u8; 36];
    assert_eq!(fd.read(&mut txt).expect("read failed"), txt.len());
    assert_eq!(
        std::str::from_utf8(&txt).unwrap(),
        "hello worldi worldhello worldi world"
    );

    fd.seek(io::SeekFrom::Start(1)).expect("seek failed");
    assert_eq!(fd.read(&mut txt[0..35]).expect("read failed"), 35);
    assert_eq!(
        std::str::from_utf8(&txt).unwrap(),
        "ello worldi worldhello worldi worldd"
    );

    fd.write("hello world".as_bytes()).expect_err("expected write error");
}
