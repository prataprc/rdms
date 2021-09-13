use arbitrary::Arbitrary;

#[allow(unused_imports)]
use crate::Mdb;

/// Write operations allowed on [Mdb] index.
///
/// Passed as argument to [Mdb::write] method.
///
/// * Optional `cas`, when supplied should match with key's current
///   sequence-number. If key is missing from index, `cas` must be supplied
///   as ZERO.
/// * Optional `seqno`, when supplied shall be used as mutation's sequence
///   number, ignoring index's monotonically increasing sequence-number.
///   Typically used while replaying operations from external entities like
///   Write-Ahead-Logs.
#[derive(Clone, Arbitrary)]
pub enum Write<K, V> {
    /// Refer to Mdb::set.
    Set {
        key: K,
        value: V,
        cas: Option<u64>,
        seqno: Option<u64>,
    },
    /// Refer to Mdb::insert.
    Ins {
        key: K,
        value: V,
        cas: Option<u64>,
        seqno: Option<u64>,
    },
    /// Refer to Mdb::delete.
    Del {
        key: K,
        cas: Option<u64>,
        seqno: Option<u64>,
    },
    /// Refer to Mdb::remove.
    Rem {
        key: K,
        cas: Option<u64>,
        seqno: Option<u64>,
    },
}

impl<K, V> Write<K, V> {
    #[inline]
    pub fn set(key: K, value: V) -> Write<K, V> {
        Write::Set {
            key,
            value,
            cas: None,
            seqno: None,
        }
    }

    #[inline]
    pub fn insert(key: K, value: V) -> Write<K, V> {
        Write::Ins {
            key,
            value,
            cas: None,
            seqno: None,
        }
    }

    #[inline]
    pub fn remove(key: K) -> Write<K, V> {
        Write::Rem {
            key,
            cas: None,
            seqno: None,
        }
    }

    #[inline]
    pub fn delete(key: K) -> Write<K, V> {
        Write::Del {
            key,
            cas: None,
            seqno: None,
        }
    }

    pub fn set_seqno(self, seqno: u64) -> Write<K, V> {
        use Write::*;

        match self {
            Set {
                key, value, cas, ..
            } => Set {
                key,
                value,
                cas,
                seqno: Some(seqno),
            },
            Ins {
                key, value, cas, ..
            } => Ins {
                key,
                value,
                cas,
                seqno: Some(seqno),
            },
            Del { key, cas, .. } => Del {
                key,
                cas,
                seqno: Some(seqno),
            },
            Rem { key, cas, .. } => Rem {
                key,
                cas,
                seqno: Some(seqno),
            },
        }
    }

    pub fn set_cas(self, cas: u64) -> Write<K, V> {
        use Write::*;

        match self {
            Set {
                key, value, seqno, ..
            } => Set {
                key,
                value,
                seqno,
                cas: Some(cas),
            },
            Ins {
                key, value, seqno, ..
            } => Ins {
                key,
                value,
                seqno,
                cas: Some(cas),
            },
            Del { key, seqno, .. } => Del {
                key,
                seqno,
                cas: Some(cas),
            },
            Rem { key, seqno, .. } => Rem {
                key,
                seqno,
                cas: Some(cas),
            },
        }
    }
}
