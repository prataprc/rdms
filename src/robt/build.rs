use cbordata::{self as cbor, Cbor, IntoCbor};

use std::{cell::RefCell, convert::TryFrom, marker, rc::Rc};

use crate::{
    dbs,
    robt::{self, Config, Entry, Flusher},
    util, Error, Result,
};

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

pub struct BuildMM<K, V, I>
where
    V: dbs::Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    m_blocksize: usize,
    iflush: Rc<RefCell<Flusher>>,
    iter: Box<BuildIter<K, V, I>>,
    entry: Option<(K, u64)>,

    _val: marker::PhantomData<V>,
}

impl<K, V, I> BuildMM<K, V, I>
where
    V: dbs::Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    pub fn new(
        config: &Config,
        iflush: Rc<RefCell<Flusher>>,
        iter: BuildIter<K, V, I>,
    ) -> Self {
        BuildMM {
            m_blocksize: config.m_blocksize,
            iflush,
            iter: Box::new(iter),
            entry: None,

            _val: marker::PhantomData,
        }
    }
}

impl<K, V, I> Iterator for BuildMM<K, V, I>
where
    K: Clone + IntoCbor,
    V: IntoCbor + dbs::Diff,
    <V as dbs::Diff>::Delta: IntoCbor,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut mblock = Vec::with_capacity(self.m_blocksize);
        let block_size = self.m_blocksize.saturating_sub(1);

        let mut first_key: Option<K> = None;
        let mut curr_fpos = None;
        let mut n = 0;

        iter_result!(Cbor::Major4(cbor::Info::Indefinite, vec![]).encode(&mut mblock));

        loop {
            let entry = {
                let entry = self.entry.take().map(|e| Some(Ok(e)));
                entry.unwrap_or_else(|| self.iter.next())
            };
            match entry {
                Some(Ok((key, fpos))) => {
                    curr_fpos = Some(fpos);
                    n += 1;

                    first_key.get_or_insert_with(|| key.clone());
                    let ibytes = {
                        let e = robt::Entry::<K, V>::new_mm(key.clone(), fpos);
                        iter_result!(util::into_cbor_bytes(e))
                    };
                    if (mblock.len() + ibytes.len()) > block_size {
                        self.entry = Some((key, fpos));
                        break;
                    }
                    mblock.extend_from_slice(&ibytes);
                }
                Some(Err(err)) => return Some(Err(err)),
                None if first_key.is_some() => break,
                None => return None,
            }
        }

        let brk = iter_result!(util::into_cbor_bytes(cbor::SimpleValue::Break));
        mblock.extend_from_slice(&brk);
        // println!("mmblock len:{} n:{}", mblock.len(), n);
        mblock.resize(self.m_blocksize, 0);

        if n > 1 {
            curr_fpos = Some(self.iflush.borrow().to_fpos().unwrap_or(0));
            iter_result!(self.iflush.borrow_mut().flush(mblock));
        }

        Some(Ok((first_key.unwrap(), curr_fpos.unwrap())))
    }
}

pub struct BuildMZ<K, V, I>
where
    V: dbs::Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    m_blocksize: usize,
    iflush: Rc<RefCell<Flusher>>,
    iter: BuildZZ<K, V, I>,
    entry: Option<(K, u64)>,

    _val: marker::PhantomData<V>,
}

impl<K, V, I> BuildMZ<K, V, I>
where
    V: dbs::Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    pub fn new(
        config: &Config,
        iflush: Rc<RefCell<Flusher>>,
        iter: BuildZZ<K, V, I>,
    ) -> Self {
        BuildMZ {
            m_blocksize: config.m_blocksize,
            iflush,
            iter,
            entry: None,

            _val: marker::PhantomData,
        }
    }
}

impl<K, V, I> Iterator for BuildMZ<K, V, I>
where
    K: Clone + IntoCbor,
    V: IntoCbor + dbs::Diff,
    <V as dbs::Diff>::Delta: IntoCbor,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut mblock = Vec::with_capacity(self.m_blocksize);
        let block_size = self.m_blocksize.saturating_sub(1);

        let mut first_key: Option<K> = None;

        iter_result!(Cbor::Major4(cbor::Info::Indefinite, vec![]).encode(&mut mblock));

        loop {
            let entry = {
                let entry = self.entry.take().map(|e| Some(Ok(e)));
                entry.unwrap_or_else(|| self.iter.next())
            };
            match entry {
                Some(Ok((key, fpos))) => {
                    first_key.get_or_insert_with(|| key.clone());
                    let ibytes = {
                        let e = robt::Entry::<K, V>::new_mz(key.clone(), fpos);
                        iter_result!(util::into_cbor_bytes(e))
                    };
                    if (mblock.len() + ibytes.len()) > block_size {
                        self.entry = Some((key, fpos));
                        break;
                    }
                    mblock.extend_from_slice(&ibytes);
                }
                Some(Err(err)) => return Some(Err(err)),
                None if first_key.is_some() => break,
                None => return None,
            }
        }

        let brk = iter_result!(util::into_cbor_bytes(cbor::SimpleValue::Break));
        mblock.extend_from_slice(&brk);
        // println!("mzblock len:{} start..:{:?}", mblock.len(), &mblock[..32]);
        mblock.resize(self.m_blocksize, 0);

        let fpos = self.iflush.borrow().to_fpos().unwrap_or(0);

        iter_result!(self.iflush.borrow_mut().flush(mblock));
        Some(Ok((first_key.unwrap(), fpos)))
    }
}

pub struct BuildZZ<K, V, I>
where
    V: dbs::Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    z_blocksize: usize,
    v_blocksize: usize,
    value_in_vlog: bool,
    delta_ok: bool,
    iflush: Rc<RefCell<Flusher>>,
    vflush: Rc<RefCell<Flusher>>,
    entry: Option<Result<Entry<K, V>>>,
    iter: Rc<RefCell<I>>,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, I> BuildZZ<K, V, I>
where
    V: dbs::Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    pub fn new(
        config: &Config,
        iflush: Rc<RefCell<Flusher>>,
        vflush: Rc<RefCell<Flusher>>,
        iter: Rc<RefCell<I>>,
    ) -> Self {
        BuildZZ {
            z_blocksize: config.z_blocksize,
            v_blocksize: config.v_blocksize,
            value_in_vlog: config.value_in_vlog,
            delta_ok: config.delta_ok,
            iflush,
            vflush,
            entry: None,
            iter,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }
}

impl<K, V, I> Iterator for BuildZZ<K, V, I>
where
    K: Clone + IntoCbor,
    V: IntoCbor + dbs::Diff,
    <V as dbs::Diff>::Delta: IntoCbor,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut zblock = Vec::with_capacity(self.z_blocksize);
        let mut vblock = Vec::with_capacity(self.v_blocksize);
        let block_size = self.z_blocksize.saturating_sub(1);

        let mut first_key: Option<K> = None;

        iter_result!(Cbor::Major4(cbor::Info::Indefinite, vec![]).encode(&mut zblock));

        let mut iter = self.iter.borrow_mut();
        let mut vfpos = self.vflush.borrow().to_fpos().unwrap_or(0);

        loop {
            let entry = match self.entry.take() {
                Some(entry) => Some(entry),
                None => iter.next(),
            };
            match entry {
                Some(Ok(mut entry)) => {
                    //match &entry {
                    //    Entry::MM { .. } | Entry::MZ { .. } => unreachable!(),
                    //    Entry::ZZ { deltas, .. } => println!("{}", deltas.len()),
                    //}
                    if !self.delta_ok {
                        entry.drain_deltas()
                    }
                    first_key.get_or_insert_with(|| entry.as_key().clone());
                    let (e, vbytes) = iter_result!(entry
                        .clone()
                        .into_reference(vfpos, self.value_in_vlog));
                    let ibytes = iter_result!(util::into_cbor_bytes(e));

                    if (zblock.len() + ibytes.len()) > block_size {
                        self.entry = Some(Ok(entry));
                        break;
                    }
                    zblock.extend_from_slice(&ibytes);
                    vblock.extend_from_slice(&vbytes);
                    vfpos += u64::try_from(vbytes.len()).unwrap();
                }
                Some(Err(err)) => return Some(Err(err)),
                None if first_key.is_some() => break,
                None => return None,
            }
        }

        let brk = iter_result!(util::into_cbor_bytes(cbor::SimpleValue::Break));
        zblock.extend_from_slice(&brk);
        // println!("zblock {}", zblock.len());
        zblock.resize(self.z_blocksize, 0);

        let fpos = self.iflush.borrow().to_fpos().unwrap_or(0);

        iter_result!(self.vflush.borrow_mut().flush(vblock));
        iter_result!(self.iflush.borrow_mut().flush(zblock));
        Some(Ok((first_key.unwrap(), fpos)))
    }
}

pub enum BuildIter<K, V, I>
where
    V: dbs::Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    MM(BuildMM<K, V, I>),
    MZ(BuildMZ<K, V, I>),
}

impl<K, V, I> From<BuildMZ<K, V, I>> for BuildIter<K, V, I>
where
    V: dbs::Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    fn from(val: BuildMZ<K, V, I>) -> Self {
        BuildIter::MZ(val)
    }
}

impl<K, V, I> From<BuildMM<K, V, I>> for BuildIter<K, V, I>
where
    V: dbs::Diff,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    fn from(val: BuildMM<K, V, I>) -> Self {
        BuildIter::MM(val)
    }
}

impl<K, V, I> Iterator for BuildIter<K, V, I>
where
    K: Clone + IntoCbor,
    V: IntoCbor + dbs::Diff,
    <V as dbs::Diff>::Delta: IntoCbor,
    I: Iterator<Item = Result<Entry<K, V>>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BuildIter::MM(iter) => iter.next(),
            BuildIter::MZ(iter) => iter.next(),
        }
    }
}
