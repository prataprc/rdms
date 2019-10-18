use fs2::FileExt;

use std::{fs, io};

#[test]
fn test_write_lock() {
    let file = {
        let mut dir = std::env::temp_dir();
        dir.push("fs2-test-write-lock.data");
        dir.into_os_string()
    };
    println!("lock experiment with file {:?}", file);
    fs::remove_file(&file).ok();

    let fd1 = {
        let mut opts = fs::OpenOptions::new();
        opts.append(true)
            .create_new(true)
            .open(file.clone())
            .unwrap()
    };
    fd1.lock_shared().unwrap();

    let fd2 = {
        let mut opts = fs::OpenOptions::new();
        opts.write(true).open(file.clone()).unwrap()
    };
    match fd2.try_lock_exclusive() {
        Ok(_) => panic!("unexpected behaviour!!"),
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => (),
        Err(err) => panic!("unexpected err: {:?}", err),
    }

    fd2.lock_shared().unwrap();

    fd2.unlock();
    match fd2.try_lock_exclusive() {
        Ok(_) => panic!("unexpected behaviour!!"),
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => (),
        Err(err) => panic!("unexpected err: {:?}", err),
    }

    fd1.unlock();

    match fd2.try_lock_exclusive() {
        Ok(_) => (),
        Err(err) => panic!("unexpected err: {:?}", err),
    }
    match fd1.try_lock_exclusive() {
        Ok(_) => panic!("unexpected behaviour!!"),
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => (),
        Err(err) => panic!("unexpected err: {:?}", err),
    }
    match fd1.try_lock_shared() {
        Ok(_) => panic!("unexpected behaviour!!"),
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => (),
        Err(err) => panic!("unexpected err: {:?}", err),
    }

    std::thread::sleep(std::time::Duration::new(1000, 0));
    fd2.unlock();
}
