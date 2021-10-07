use cbordata::FromCbor;
use fs2::FileExt;

use std::{
    borrow::Borrow,
    cmp,
    convert::TryFrom,
    fmt, fs,
    io::{self, Read, Seek},
    ops::{Bound, RangeBounds},
    rc::Rc,
};

use crate::{
    db, read_file,
    robt::{self, Stats},
    util, Error, Result,
};

// TODO: check for panic!()

macro_rules! iter_result {
    ($res:expr) => {{
        match $res {
            Ok(res) => res,
            Err(err) => {
                let prefix = format!("{}:{}", file!(), line!());
                return Some(Err(Error::FailCbor(prefix, format!("{}", err))));
            }
        }
    }};
}

pub struct Reader<K, V>
where
    V: db::Diff,
{
    pub m_blocksize: usize,
    pub z_blocksize: usize,
    pub root: Rc<Vec<robt::Entry<K, V>>>,

    pub index: fs::File,
    pub vlog: Option<fs::File>,
}

impl<K, V> Drop for Reader<K, V>
where
    V: db::Diff,
{
    fn drop(&mut self) {
        // println!("reader unlock >");
        if let Err(err) = self.index.unlock() {
            panic!("fail to unlock reader lock for index: {}", err)
        }
        if let Some(vlog) = self.vlog.as_ref() {
            // println!("reader unlock >");
            if let Err(err) = vlog.unlock() {
                panic!("fail to unlock reader lock for vlog: {}", err)
            }
        }
    }
}

impl<K, V> Reader<K, V>
where
    K: FromCbor,
    V: db::Diff + FromCbor,
    <V as db::Diff>::Delta: FromCbor,
{
    pub fn from_root(
        root: u64,
        stats: &Stats,
        mut index: fs::File,
        vlog: Option<fs::File>,
    ) -> Result<Self> {
        let root: Vec<robt::Entry<K, V>> = {
            let fpos = io::SeekFrom::Start(root);
            let block = read_file!(&mut index, fpos, stats.m_blocksize, "read block")?;
            // println!("read root fpos:{:?} len:{}", fpos, block.len());
            util::from_cbor_bytes(&block)?.0
        };
        // println!("read root:{}", root.len());

        // println!("reader.from_root lock_shared <");
        err_at!(IOError, index.lock_shared())?;
        if let Some(vlog) = vlog.as_ref() {
            // println!("reader.from_root lock_shared <");
            err_at!(IOError, vlog.lock_shared())?
        }

        Ok(Reader {
            m_blocksize: stats.m_blocksize,
            z_blocksize: stats.z_blocksize,
            root: Rc::new(root),

            index,
            vlog,
        })
    }

    pub fn as_root(&self) -> Rc<Vec<robt::Entry<K, V>>> {
        Rc::clone(&self.root)
    }

    pub fn get<Q>(&mut self, ukey: &Q, versions: bool) -> Result<robt::Entry<K, V>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        Q: Ord,
    {
        let m_blocksize = self.m_blocksize;
        let z_blocksize = self.z_blocksize;
        let fd = &mut self.index;

        let mut es = Rc::clone(&self.root);
        loop {
            let off = match es.binary_search_by(|e| e.borrow_key().cmp(ukey)) {
                Ok(off) => off,
                Err(off) if off == 0 => break err_at!(KeyNotFound, msg: "missing key"),
                Err(off) => off - 1,
            };
            es = match es[off].clone() {
                robt::Entry::MM { fpos, .. } => {
                    let fpos = io::SeekFrom::Start(fpos);
                    let block = read_file!(fd, fpos, m_blocksize, "read mm-block")?;
                    Rc::new(util::from_cbor_bytes::<Vec<robt::Entry<K, V>>>(&block)?.0)
                }
                robt::Entry::MZ { fpos, .. } => {
                    let fpos = io::SeekFrom::Start(fpos);
                    let block = read_file!(fd, fpos, z_blocksize, "read mz-block")?;
                    Rc::new(util::from_cbor_bytes::<Vec<robt::Entry<K, V>>>(&block)?.0)
                }
                robt::Entry::ZZ { key, value, deltas } if key.borrow() == ukey => {
                    let deltas = if versions { deltas } else { Vec::default() };
                    let mut entry = robt::Entry::ZZ { key, value, deltas };
                    let entry = match &mut self.vlog {
                        Some(fd) => entry.into_native(fd, versions)?,
                        None => {
                            entry.drain_deltas();
                            entry
                        }
                    };
                    break Ok(entry);
                }
                _ => break err_at!(KeyNotFound, msg: "missing key"),
            }
        }
    }

    pub fn iter<Q, R>(
        &mut self,
        range: R,
        reverse: bool,
        versions: bool,
    ) -> Result<Iter<K, V>>
    where
        K: Clone + Ord + Borrow<Q>,
        V: Clone,
        Q: Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let (stack, bound) = if reverse {
            let stack = self.rwd_stack(range.end_bound(), Rc::clone(&self.root))?;
            let bound: Bound<K> = match range.start_bound() {
                Bound::Unbounded => Bound::Unbounded,
                Bound::Included(q) => Bound::Included(q.to_owned()),
                Bound::Excluded(q) => Bound::Excluded(q.to_owned()),
            };
            (stack, bound)
        } else {
            let stack = self.fwd_stack(range.start_bound(), Rc::clone(&self.root))?;
            // println!("iter stack:{:?}", stack.len());
            let bound: Bound<K> = match range.end_bound() {
                Bound::Unbounded => Bound::Unbounded,
                Bound::Included(q) => Bound::Included(q.to_owned()),
                Bound::Excluded(q) => Bound::Excluded(q.to_owned()),
            };
            (stack, bound)
        };
        let mut iter = Iter::new(self, bound, stack, reverse, versions);

        while let Some(item) = iter.next() {
            match item {
                Ok(entry) if reverse => {
                    let key = entry.borrow_key();
                    match range.end_bound() {
                        Bound::Included(ekey) if key.gt(ekey) => (),
                        Bound::Excluded(ekey) if key.ge(ekey) => (),
                        _ => {
                            iter.push(entry);
                            break;
                        }
                    }
                }
                Ok(entry) => {
                    let key = entry.borrow_key();
                    match range.start_bound() {
                        Bound::Included(skey) if key.lt(skey) => (),
                        Bound::Excluded(skey) if key.le(skey) => (),
                        _ => {
                            iter.push(entry);
                            break;
                        }
                    }
                }
                Err(err) => return Err(err),
            }
        }

        Ok(iter)
    }

    pub fn fwd_stack<Q>(
        &mut self,
        sk: Bound<&Q>,
        block: Rc<Vec<robt::Entry<K, V>>>,
    ) -> Result<Vec<Vec<robt::Entry<K, V>>>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        Q: Ord,
    {
        // println!("fwd_stack block_len:{}", block.len());
        let (entry, rem) = match block.first().map(|e| e.is_zblock()) {
            Some(false) => match block.binary_search_by(|e| fcmp(e.borrow_key(), sk)) {
                Ok(off) => (block[off].clone(), block[off + 1..].to_vec()),
                Err(off) => {
                    let off = off.saturating_sub(1);
                    (block[off].clone(), block[off + 1..].to_vec())
                }
            },
            Some(true) => match block.binary_search_by(|e| fcmp(e.borrow_key(), sk)) {
                Ok(off) | Err(off) => {
                    return Ok(vec![block[off..].to_vec()]);
                }
            },
            None => return Ok(vec![]),
        };

        let fd = &mut self.index;
        let m_blocksize = self.m_blocksize;
        let z_blocksize = self.z_blocksize;

        let block = match entry {
            robt::Entry::MM { fpos, .. } => {
                // println!("mm-entry fpos:{}", fpos);
                read_file!(fd, io::SeekFrom::Start(fpos), m_blocksize, "read mm-block")?
            }
            robt::Entry::MZ { fpos, .. } => {
                // println!("mz-entry fpos:{}", fpos);
                read_file!(fd, io::SeekFrom::Start(fpos), z_blocksize, "read mz-block")?
            }
            _ => unreachable!(),
        };
        // println!("read-block len:{} start..:{:?}", block.len(), &block[..32]);

        let block = Rc::new(util::from_cbor_bytes::<Vec<robt::Entry<K, V>>>(&block)?.0);
        let mut stack = self.fwd_stack(sk, block)?;
        stack.insert(0, rem);
        Ok(stack)
    }

    fn rwd_stack<Q>(
        &mut self,
        ek: Bound<&Q>,
        block: Rc<Vec<robt::Entry<K, V>>>,
    ) -> Result<Vec<Vec<robt::Entry<K, V>>>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        Q: Ord,
    {
        let (entry, mut rem) = match block.first().map(|e| e.is_zblock()) {
            Some(false) => match block.binary_search_by(|e| rcmp(e.borrow_key(), ek)) {
                Ok(off) => (block[off].clone(), block[..off].to_vec()),
                Err(off) => {
                    let off = off.saturating_sub(1);
                    (block[off].clone(), block[..off].to_vec())
                }
            },
            Some(true) => match block.binary_search_by(|e| rcmp(e.borrow_key(), ek)) {
                Ok(off) | Err(off) => {
                    let off = cmp::min(off + 1, block.len());
                    let mut rem = block[..off].to_vec();
                    rem.reverse();
                    return Ok(vec![rem]);
                }
            },
            None => return Ok(vec![]),
        };
        rem.reverse();

        let fd = &mut self.index;
        let m_blocksize = self.m_blocksize;
        let z_blocksize = self.z_blocksize;

        let block = match entry {
            robt::Entry::MM { fpos, .. } => {
                read_file!(fd, io::SeekFrom::Start(fpos), m_blocksize, "read mm-block")?
            }
            robt::Entry::MZ { fpos, .. } => {
                read_file!(fd, io::SeekFrom::Start(fpos), z_blocksize, "read mz-block")?
            }
            _ => unreachable!(),
        };

        let block = Rc::new(util::from_cbor_bytes::<Vec<robt::Entry<K, V>>>(&block)?.0);
        let mut stack = self.rwd_stack(ek, block)?;
        stack.insert(0, rem);
        Ok(stack)
    }

    pub fn print(&mut self) -> Result<()>
    where
        K: Clone + fmt::Debug,
        V: Clone + fmt::Debug,
        <V as db::Diff>::Delta: fmt::Debug,
    {
        for entry in self.root.to_vec().into_iter() {
            entry.print("", self)?;
        }
        Ok(())
    }
}

pub struct Iter<'a, K, V>
where
    V: db::Diff,
{
    reader: &'a mut Reader<K, V>,
    stack: Vec<Vec<robt::Entry<K, V>>>,
    reverse: bool,
    versions: bool,
    entry: Option<db::Entry<K, V>>,
    bound: Bound<K>,
}

impl<'a, K, V> Iter<'a, K, V>
where
    V: db::Diff,
{
    fn new(
        r: &'a mut Reader<K, V>,
        bound: Bound<K>,
        stack: Vec<Vec<robt::Entry<K, V>>>,
        reverse: bool,
        versions: bool,
    ) -> Self {
        Iter {
            reader: r,
            stack,
            reverse,
            versions,
            entry: None,
            bound,
        }
    }

    fn push(&mut self, entry: db::Entry<K, V>) {
        self.entry = Some(entry);
    }

    fn till(&mut self, e: db::Entry<K, V>) -> Option<Result<db::Entry<K, V>>>
    where
        K: Ord,
    {
        let key = &e.key;

        if self.reverse {
            match &self.bound {
                Bound::Unbounded => Some(Ok(e)),
                Bound::Included(till) if key.ge(till) => Some(Ok(e)),
                Bound::Excluded(till) if key.gt(till) => Some(Ok(e)),
                _ => {
                    self.stack.drain(..);
                    None
                }
            }
        } else {
            match &self.bound {
                Bound::Unbounded => Some(Ok(e)),
                Bound::Included(till) if key.le(till) => Some(Ok(e)),
                Bound::Excluded(till) if key.lt(till) => Some(Ok(e)),
                _ => {
                    self.stack.drain(..);
                    None
                }
            }
        }
    }

    fn fetchzz(&mut self, mut entry: robt::Entry<K, V>) -> Result<robt::Entry<K, V>>
    where
        V: FromCbor,
        <V as db::Diff>::Delta: FromCbor,
    {
        match &mut self.reader.vlog {
            Some(fd) if self.versions => entry.into_native(fd, self.versions),
            Some(fd) => {
                entry.drain_deltas();
                entry.into_native(fd, self.versions)
            }
            None => {
                entry.drain_deltas();
                Ok(entry)
            }
        }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Ord + FromCbor,
    V: db::Diff + FromCbor,
    <V as db::Diff>::Delta: FromCbor,
{
    type Item = Result<db::Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.entry.take() {
            return Some(Ok(entry));
        }

        let fd = &mut self.reader.index;
        let m_blocksize = self.reader.m_blocksize;
        let z_blocksize = self.reader.z_blocksize;

        match self.stack.pop() {
            Some(block) if block.is_empty() => self.next(),
            Some(mut block) => match block.remove(0) {
                entry @ robt::Entry::ZZ { .. } => {
                    self.stack.push(block);
                    let entry = iter_result!(self.fetchzz(entry));
                    match db::Entry::try_from(entry) {
                        Ok(entry) => self.till(entry),
                        err => Some(err),
                    }
                }
                robt::Entry::MM { fpos, .. } => {
                    self.stack.push(block);

                    let mut entries =
                        iter_result!(|| -> Result<Vec<robt::Entry<K, V>>> {
                            let fpos = io::SeekFrom::Start(fpos);
                            let block =
                                read_file!(fd, fpos, m_blocksize, "read mm-block")?;
                            Ok(util::from_cbor_bytes(&block)?.0)
                        }());
                    if self.reverse {
                        entries.reverse();
                    }
                    self.stack.push(entries);
                    self.next()
                }
                robt::Entry::MZ { fpos, .. } => {
                    self.stack.push(block);

                    let mut entries =
                        iter_result!(|| -> Result<Vec<robt::Entry<K, V>>> {
                            let fpos = io::SeekFrom::Start(fpos);
                            let block =
                                read_file!(fd, fpos, z_blocksize, "read mm-block")?;
                            Ok(util::from_cbor_bytes(&block)?.0)
                        }());
                    if self.reverse {
                        entries.reverse();
                    }
                    self.stack.push(entries);
                    self.next()
                }
            },
            None => None,
        }
    }
}

pub struct IterLsm<'a, K, V>
where
    V: db::Diff,
{
    reader: &'a mut Reader<K, V>,
    stack: Vec<Vec<robt::Entry<K, V>>>,
    versions: bool,
}

impl<'a, K, V> IterLsm<'a, K, V>
where
    V: db::Diff,
{
    pub fn new(
        r: &'a mut Reader<K, V>,
        stack: Vec<Vec<robt::Entry<K, V>>>,
        versions: bool,
    ) -> Self {
        IterLsm {
            reader: r,
            stack,
            versions,
        }
    }

    fn fetchzz(&mut self, mut entry: robt::Entry<K, V>) -> Result<robt::Entry<K, V>>
    where
        V: FromCbor,
        <V as db::Diff>::Delta: FromCbor,
    {
        match &mut self.reader.vlog {
            Some(fd) if self.versions => entry.into_native(fd, self.versions),
            Some(fd) => {
                entry.drain_deltas();
                entry.into_native(fd, self.versions)
            }
            None => {
                entry.drain_deltas();
                Ok(entry)
            }
        }
    }
}

impl<'a, K, V> Iterator for IterLsm<'a, K, V>
where
    K: Ord + FromCbor,
    V: db::Diff + FromCbor,
    <V as db::Diff>::Delta: FromCbor,
{
    type Item = Result<robt::Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        let fd = &mut self.reader.index;
        let m_blocksize = self.reader.m_blocksize;
        let z_blocksize = self.reader.z_blocksize;

        match self.stack.pop() {
            Some(block) if block.is_empty() => self.next(),
            Some(mut block) => match block.remove(0) {
                entry @ robt::Entry::ZZ { .. } => {
                    self.stack.push(block);
                    Some(Ok(iter_result!(self.fetchzz(entry))))
                }
                robt::Entry::MM { fpos, .. } => {
                    self.stack.push(block);

                    let entries = iter_result!(|| -> Result<Vec<robt::Entry<K, V>>> {
                        let fpos = io::SeekFrom::Start(fpos);
                        let block = read_file!(fd, fpos, m_blocksize, "read mm-block")?;
                        Ok(util::from_cbor_bytes(&block)?.0)
                    }());

                    self.stack.push(entries);
                    self.next()
                }
                robt::Entry::MZ { fpos, .. } => {
                    self.stack.push(block);

                    let entries = iter_result!(|| -> Result<Vec<robt::Entry<K, V>>> {
                        let fpos = io::SeekFrom::Start(fpos);
                        let block = read_file!(fd, fpos, z_blocksize, "read mm-block")?;
                        Ok(util::from_cbor_bytes(&block)?.0)
                    }());

                    self.stack.push(entries);
                    self.next()
                }
            },
            None => None,
        }
    }
}

fn fcmp<Q>(key: &Q, skey: Bound<&Q>) -> cmp::Ordering
where
    Q: Ord,
{
    match skey {
        Bound::Unbounded => cmp::Ordering::Greater,
        Bound::Included(skey) | Bound::Excluded(skey) => key.cmp(skey),
    }
}

fn rcmp<Q>(key: &Q, ekey: Bound<&Q>) -> cmp::Ordering
where
    Q: Ord,
{
    match ekey {
        Bound::Unbounded => cmp::Ordering::Less,
        Bound::Included(ekey) | Bound::Excluded(ekey) => key.cmp(ekey),
    }
}
