use arbitrary::Unstructured;
use rand::{prelude::random, rngs::StdRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_wral_entry() {
    use cbordata::{Cbor, FromCbor, IntoCbor};

    let seed: u64 = random();
    let mut rng = StdRng::seed_from_u64(seed);
    println!("test_wral_entry {}", seed);

    let mut entries: Vec<Entry> = (0..1000)
        .map(|_i| {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary::<Entry>().unwrap()
        })
        .collect();
    entries.sort();
    entries.dedup_by(|a, b| a.seqno == b.seqno);

    for entry in entries.iter() {
        let entry = entry.clone();
        assert_eq!(entry.to_seqno(), entry.seqno);
        let (seqno, op) = entry.clone().unwrap();
        assert_eq!(entry, Entry::new(seqno, op));

        let cbor: Cbor = entry.clone().into_cbor().unwrap();
        let mut buf: Vec<u8> = vec![];
        let n = cbor.encode(&mut buf).unwrap();
        let (val, m) = Cbor::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(n, m);
        assert_eq!(cbor, val);

        let entr = Entry::from_cbor(val).unwrap();
        assert_eq!(entr, entry);
    }

    let mut seqno = 0;
    for entry in entries.into_iter() {
        assert!(seqno < entry.seqno, "{} {}", seqno, entry.seqno);
        seqno = entry.seqno
    }
}
