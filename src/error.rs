//! Module error define enumeration of `rdms` errors and convertion
//! traits from other error types, like from std-lib, to `rdms` error.

use jsondata;

use std::{any, ffi, io, sync::mpsc};

// TODO: check unused error variants and double check error arguments.
// TODO: Generic but meaningful error messages.
// TODO: Document error variants.
// TODO: Consolidate convertion traits from other error types.

/// Error enumerates over all possible errors cases in `rdms` package.
#[derive(Debug)]
pub enum Error {
    /// API / function not supported
    NotSupported(String),
    /// Supplied key is not found in the index.
    KeyNotFound,
    /// Can be returned by set_cas() API when:
    /// * In non-lsm mode, requested entry is missing but specified
    ///   CAS is not ZERO. Note that this combination is an alias for
    ///   create-only operation.
    /// * In lsm mode, requested entry is marked as deleted, and
    ///   specifed CAS is neither ZERO, nor matching with entry's
    ///   last modified sequence-number.
    /// * Requested entry's last modified sequence-number does not
    ///   match with specified CAS.
    InvalidCAS,
    /// Key size, after serializing, has exceeded the configured,
    /// (or hard coded) limit.
    KeySizeExceeded(usize),
    /// Value size, after serializing, has exceed the configured,
    /// (or hard coded) limit.
    ValueSizeExceeded(usize),
    /// Value-diff size, after serializing, exceeds limit.
    DiffSizeExceeded(usize),
    /// Index has failed to meet the validation criteria. String
    /// argument contains more details.
    ValidationFail(String),
    /// Index has failed.
    DiskIndexFail(String),
    /// Expected a native value. TODO: hide this ?
    NotNativeValue,
    /// Expected a native delta. TODO: hide this ?
    NotNativeDelta,
    /// De-serialization failed.
    DecodeFail(String),
    /// Unable to read expected bytes from file.
    PartialRead(String),
    /// Unable to write full buffer into file.
    PartialWrite(String),
    /// Returned by disk index or Wal that provide durability support.
    InvalidFile(String),
    /// Thread has failed.
    ThreadFail(String),
    /// On disk snapshot is invalid.
    InvalidSnapshot(String),
    /// Inter-Process-Communication error from std::mpsc
    IPCFail(String),
    /// Invalid WAL
    InvalidWAL(String),
    /// IO error from std::io
    IoError(io::Error),
    /// Json processing error from jsondata package
    JsonError(jsondata::Error),
    /// String conversion error from std::String, str::str
    Utf8Error(std::str::Utf8Error),
    // internal error, given key is less than the entire data set.
    __LessThan,
    // internal error, z-block of robt index has overflowed.
    __ZBlockOverflow(usize),
    // inernal error, m-block of robt index has overflowed.
    __MBlockOverflow(usize),
    // internal error, iteration exhausted in robt index's m-block entries.
    __MBlockExhausted(usize),
    // internal error, iteration exhausted in robt index's z-block entries.
    __ZBlockExhausted(usize),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<ffi::OsString> for Error {
    fn from(err: ffi::OsString) -> Error {
        Error::InvalidFile(format!("{:?}", err))
    }
}

impl<T> From<mpsc::SendError<T>> for Error {
    fn from(err: mpsc::SendError<T>) -> Error {
        let msg = format!("SendError: {:?}", err);
        Error::IPCFail(msg)
    }
}

impl From<mpsc::RecvError> for Error {
    fn from(err: mpsc::RecvError) -> Error {
        let msg = format!("RecvError: {:?}", err);
        Error::IPCFail(msg)
    }
}

impl From<Box<dyn any::Any + Send>> for Error {
    fn from(err: Box<dyn any::Any + Send>) -> Error {
        let msg = format!("dynamic error: {:?}", err);
        Error::InvalidWAL(msg)
    }
}

impl From<jsondata::Error> for Error {
    fn from(err: jsondata::Error) -> Error {
        Error::JsonError(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Error {
        Error::Utf8Error(err)
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        use Error::InvalidCAS;
        use Error::InvalidFile;

        match (self, other) {
            (InvalidCAS, InvalidCAS) => true,
            (InvalidFile(s1), InvalidFile(s2)) => s1 == s2,
            _ => false,
        }
    }
}
