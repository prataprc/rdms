use cbordata::{self as cbor, Cbor, Cborize, FromCbor, IntoCbor};

use std::{
    borrow::Borrow,
    convert::TryFrom,
    fmt,
    io::{self, Read, Seek},
};

use crate::{
    db, read_file,
    robt::{reader::Reader, vlog},
    util, Error, Result,
};

const ENTRY_VER: u32 = 0x00130001;

#[derive(Clone, Debug, Eq, PartialEq, Cborize)]
pub enum Entry<K, V, D = <V as db::Diff>::Delta>
where
    V: db::Diff<Delta = D>,
{
    MM {
        key: K,
        fpos: u64,
    },
    MZ {
        key: K,
        fpos: u64,
    },
    ZZ {
        key: K,
        value: vlog::Value<V>,
        deltas: Vec<vlog::Delta<D>>,
    },
}

impl<K, V, D> From<db::Entry<K, V, D>> for Entry<K, V, D>
where
    V: db::Diff<Delta = D>,
{
    fn from(e: db::Entry<K, V, D>) -> Entry<K, V, D> {
        Entry::ZZ {
            key: e.key,
            value: e.value.into(),
            deltas: e.deltas.into_iter().map(vlog::Delta::from).collect(),
        }
    }
}

impl<K, V, D> TryFrom<Entry<K, V, D>> for db::Entry<K, V, D>
where
    V: db::Diff<Delta = D>,
{
    type Error = Error;

    fn try_from(e: Entry<K, V, D>) -> Result<db::Entry<K, V, D>> {
        let entry = match e {
            Entry::ZZ { key, value, deltas } => {
                let value = db::Value::try_from(value)?;
                let mut ds = vec![];
                for delta in deltas.into_iter() {
                    ds.push(db::Delta::try_from(delta)?);
                }
                db::Entry {
                    key,
                    value,
                    deltas: ds,
                }
            }
            Entry::MZ { .. } => err_at!(Fatal, msg: "robt-mz node not a leaf-node")?,
            Entry::MM { .. } => err_at!(Fatal, msg: "robt-mm node not a leaf-node")?,
        };

        Ok(entry)
    }
}

impl<K, V, D> Entry<K, V, D>
where
    V: db::Diff<Delta = D>,
{
    const ID: u32 = ENTRY_VER;

    pub fn new_mm(key: K, fpos: u64) -> Self {
        Entry::MM { key, fpos }
    }

    pub fn new_mz(key: K, fpos: u64) -> Self {
        Entry::MZ { key, fpos }
    }

    pub fn drain_deltas(&mut self) {
        match self {
            Entry::MM { .. } | Entry::MZ { .. } => (),
            Entry::ZZ { deltas, .. } => {
                deltas.drain(..);
            }
        }
    }
}

impl<K, V, D> Entry<K, V, D>
where
    V: db::Diff<Delta = D>,
{
    // serialize into value-block and return the same.
    pub fn into_reference(self, mut vfpos: u64, vlog: bool) -> Result<(Self, Vec<u8>)>
    where
        V: IntoCbor,
        D: IntoCbor,
    {
        let (entry, data) = match self {
            Entry::MM { .. } => (self, vec![]),
            Entry::MZ { .. } => (self, vec![]),
            Entry::ZZ { key, value, deltas } => {
                let (value, mut vblock) = if vlog {
                    value.into_reference(vfpos)?
                } else {
                    (value, vec![])
                };

                err_at!(
                    FailCbor,
                    Cbor::Major4(cbor::Info::Indefinite, vec![]).encode(&mut vblock)
                )?;

                vfpos += err_at!(FailConvert, u64::try_from(vblock.len()))?;

                let mut drefs = vec![];
                for delta in deltas.into_iter() {
                    let (delta, data) = delta.into_reference(vfpos)?;
                    drefs.push(delta);
                    vblock.extend_from_slice(&data);
                    vfpos += err_at!(FailConvert, u64::try_from(data.len()))?;
                }

                vblock
                    .extend_from_slice(&util::into_cbor_bytes(cbor::SimpleValue::Break)?);

                let entry = Entry::ZZ {
                    key,
                    value,
                    deltas: drefs,
                };

                (entry, vblock)
            }
        };

        Ok((entry, data))
    }

    pub fn into_native<F>(self, f: &mut F, versions: bool) -> Result<Self>
    where
        V: FromCbor,
        D: FromCbor,
        F: io::Seek + io::Read,
    {
        let entry = match self {
            Entry::MM { .. } => self,
            Entry::MZ { .. } => self,
            Entry::ZZ { key, value, deltas } if versions => {
                let native_value = value.into_native(f)?;
                let mut native_deltas = vec![];
                for delta in deltas.into_iter() {
                    native_deltas.push(delta.into_native(f)?);
                }

                let entry = Entry::ZZ {
                    key,
                    value: native_value,
                    deltas: native_deltas,
                };

                entry
            }
            Entry::ZZ { key, value, .. } => {
                let native_value = value.into_native(f)?;
                Entry::ZZ {
                    key,
                    value: native_value,
                    deltas: Vec::default(),
                }
            }
        };

        Ok(entry)
    }

    pub fn print(&self, prefix: &str, reader: &mut Reader<K, V>) -> Result<()>
    where
        K: fmt::Debug + FromCbor,
        V: fmt::Debug + FromCbor,
        <V as db::Diff>::Delta: fmt::Debug + FromCbor,
    {
        let fd = &mut reader.index;
        let entries = match self {
            Entry::MM { key, fpos } => {
                let off = io::SeekFrom::Start(*fpos);
                let block = read_file!(fd, off, reader.m_blocksize, "read mm-block")?;
                let entries = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0;
                println!("{}MM<{:?}@{},{}>", prefix, key, fpos, entries.len());
                Some(entries)
            }
            Entry::MZ { key, fpos } => {
                let off = io::SeekFrom::Start(*fpos);
                let block = read_file!(fd, off, reader.m_blocksize, "read mm-block")?;
                let entries = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0;
                println!("{}MZ<{:?}@{},{}>", prefix, key, fpos, entries.len());
                Some(entries)
            }
            Entry::ZZ { key, value, deltas } => {
                println!("{}ZZ---- key:{:?}; {:?}; {:?}", prefix, key, value, deltas);
                None
            }
        };

        let prefix = prefix.to_string() + "  ";
        if let Some(entries) = entries {
            for entry in entries.into_iter() {
                let entry = match &mut reader.vlog {
                    Some(vlog) => entry.into_native(vlog, true)?,
                    None => entry,
                };
                entry.print(prefix.as_str(), reader)?;
            }
        }

        Ok(())
    }
}

impl<K, V, D> Entry<K, V, D>
where
    V: db::Diff<Delta = D>,
{
    pub fn as_key(&self) -> &K {
        match self {
            Entry::MZ { key, .. } => key,
            Entry::MM { key, .. } => key,
            Entry::ZZ { key, .. } => key,
        }
    }

    pub fn to_key(&self) -> K
    where
        K: Clone,
    {
        match self {
            Entry::MZ { key, .. } => key.clone(),
            Entry::MM { key, .. } => key.clone(),
            Entry::ZZ { key, .. } => key.clone(),
        }
    }

    pub fn to_seqno(&self) -> Option<u64> {
        match self {
            Entry::ZZ { value, .. } => value.to_seqno(),
            Entry::MZ { .. } | Entry::MM { .. } => None,
        }
    }

    pub fn is_deleted(&self) -> Option<bool> {
        match self {
            Entry::ZZ { value, .. } => value.is_deleted(),
            Entry::MZ { .. } | Entry::MM { .. } => None,
        }
    }

    pub fn borrow_key<Q>(&self) -> &Q
    where
        K: Borrow<Q>,
    {
        match self {
            Entry::MZ { key, .. } => key.borrow(),
            Entry::MM { key, .. } => key.borrow(),
            Entry::ZZ { key, .. } => key.borrow(),
        }
    }

    pub fn is_zblock(&self) -> bool {
        match self {
            Entry::MZ { .. } => false,
            Entry::MM { .. } => false,
            Entry::ZZ { .. } => true,
        }
    }
}

#[cfg(test)]
#[path = "entry_test.rs"]
mod entry_test;
