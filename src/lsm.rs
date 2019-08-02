//! Implement get() and iter() for LSM indexes.
use std::borrow::Borrow;

use crate::core::{Diff, Entry, Result};
use crate::error::Error;

pub type LsmGet<'a, K, V, Q> = Box<dyn Fn(&'a Q) -> Result<Entry<K, V>> + 'a>;

pub fn y_get<'a, K, V, Q>(a: LsmGet<'a, K, V, Q>, b: LsmGet<'a, K, V, Q>) -> LsmGet<'a, K, V, Q>
where
    K: 'a + Clone + Ord + Borrow<Q>,
    V: 'a + Clone + Diff,
    Q: 'a + Ord + ?Sized,
{
    Box::new(move |key: &Q| -> Result<Entry<K, V>> {
        match b(key) {
            Ok(entry) => Ok(entry),
            Err(Error::KeyNotFound) => a(key),
            Err(err) => Err(err),
        }
    })
}

//pub fn y_iter<K, V>(a: IndexIter<K, V>, b: IndexIter<K, V>) -> IndexIter<K, V> {
//    let b_entry = b.next();
//    let a_entry = a.next();
//    YIter {
//        a,
//        b,
//        a_entry,
//        b_entry,
//    }
//}
//
//struct YIter<K, V> {
//    a: IndexIter<K, V>,
//    b: IndexIter<K, V>,
//    a_entry: Option<Result<Entry<K, V>>>,
//    b_entry: Option<Result<Entry<K, V>>>,
//}
//
//impl<K, V> Iterator for YIter<K, V> {
//    type Item = Result<Entry<K, V>>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        var seqno uint64
//        var del bool
//        var err error
//
//        if self.a_entry.is_error() && berr.is_error()  {
//            None
//        } else if self.a_entry.is_error() {
//        }
//
//        } else if aerr != nil {
//                key, val = cp(key, bkey), cp(val, bval)
//                seqno, del, err = bseqno, bdel, berr
//                bkey, bval, bseqno, bdel, berr = pull(b, fin, bkey, bval)
//
//        } else if berr != nil {
//                key, val = cp(key, akey), cp(val, aval)
//                seqno, del, err = aseqno, adel, aerr
//                akey, aval, aseqno, adel, aerr = pull(a, fin, akey, aval)
//
//        } else if cmp := bytes.Compare(bkey, akey); cmp < 0 {
//                key, val = cp(key, bkey), cp(val, bval)
//                seqno, del, err = bseqno, bdel, berr
//                bkey, bval, bseqno, bdel, berr = pull(b, fin, bkey, bval)
//
//        } else if cmp > 0 {
//                key, val = cp(key, akey), cp(val, aval)
//                seqno, del, err = aseqno, adel, aerr
//                akey, aval, aseqno, adel, aerr = pull(a, fin, akey, aval)
//
//        } else {
//                if bseqno > aseqno {
//                        key, val = cp(key, bkey), cp(val, bval)
//                        seqno, del, err = bseqno, bdel, berr
//                } else {
//                        key, val = cp(key, akey), cp(val, aval)
//                        seqno, del, err = aseqno, adel, aerr
//                }
//                bkey, bval, bseqno, bdel, berr = pull(b, fin, bkey, bval)
//                akey, aval, aseqno, adel, aerr = pull(a, fin, akey, aval)
//        }
//        //fmt.Printf("ysort %q %q %v %v %v\n", key, val, seqno, del, err)
//        return key, val, seqno, del, err
//    }
//}
