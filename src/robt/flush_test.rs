use arbitrary::{self, Unstructured};
use rand::{prelude::random, rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

use std::{cmp, fs, io::Read};

use super::*;

#[test]
fn test_robt_flush() {
    let seed: u64 = random();
    println!("test_flush {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let dir = std::env::temp_dir().join("test_flush");
    fs::create_dir_all(&dir).unwrap();
    let file = dir.join("test-flusher.data");
    println!("flush to file {:?}", file);
    fs::remove_file(&file).ok();

    let mut flusher = {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);

        let create = true;
        let chan_size: usize = cmp::min(uns.arbitrary().unwrap(), 12);
        Flusher::new(file.as_ref(), create, chan_size).unwrap()
    };

    let mut fpos = 0;
    let mut filedata: Vec<u8> = vec![];
    for _i in 0..1000 {
        let mut data: Vec<u8> = vec![0; 4096];
        data[..256].copy_from_slice(&(0..=255).collect::<Vec<u8>>());
        data.shuffle(&mut rng);
        filedata.extend(&data);
        flusher.flush(data).unwrap();
        fpos += 4096;
        assert_eq!(fpos, flusher.to_fpos().unwrap());
    }
    assert_eq!(flusher.close().unwrap(), 4096000);
    let mut flushed_data = vec![];
    let n = fs::OpenOptions::new()
        .read(true)
        .open(&file)
        .unwrap()
        .read_to_end(&mut flushed_data)
        .unwrap();
    assert_eq!(n, 4096000);

    assert_eq!(flushed_data, filedata);
}
