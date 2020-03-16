//! Module error define enumeration of all `rdms` errors.
//!
//! Convertion traits, from other error types, like from std-lib, to `rdms`
//! error are implemented in this module.

use jsondata;

use std::ffi;

// TODO: check unused error variants and double check error arguments.
// TODO: Generic but meaningful error messages.
// TODO: Document error variants.
// TODO: Consolidate convertion traits from other error types.

/// Error enumerates over all possible errors cases in `rdms` package.
#[derive(Debug)]
pub enum Error {
    /// Fatal failure, caller can treat this as sever as a panic!()
    Fatal(String),
    /// API / function not supported
    NotImplemented(String),
    /// Error because the value was not initialized as expected.
    UnInitialized(String),
    /// TimeFail, std time related API failed.
    TimeFail(String),
    /// Inter-Process-Communication error from std::mpsc
    IPCFail(String),
    /// System level failure.
    SystemFail(String),
    /// Invalid input from application, like function arguments,
    /// file input etc..
    InvalidInput(String),
    /// API is being misused, as in there are not invoked in
    /// suggested order/manner.
    APIMisuse(String),
    /// De-serialization failed.
    DecodeFail(String),

    /// Supplied key is not found in the index.
    KeyNotFound,
    /// Index is empty
    EmptyIndex,
    /// Can be returned by set_cas() API when:
    /// * In non-lsm mode, requested entry is missing but specified
    ///   CAS is not ZERO. Note that this combination is an alias for
    ///   create-only operation.
    /// * In lsm mode, requested entry is marked as deleted, and
    ///   specifed CAS is neither ZERO, nor matching with entry's
    ///   last modified sequence-number.
    /// * Requested entry's last modified sequence-number does not
    ///   match with specified CAS.
    InvalidCAS(u64),
    /// Key size, after serializing, has exceeded the configured,
    /// (or hard coded) limit.
    KeySizeExceeded(usize),
    /// Value size, after serializing, has exceed the configured,
    /// (or hard coded) limit.
    ValueSizeExceeded(usize),
    /// Value-diff size, after serializing, exceeds limit.
    DiffSizeExceeded(usize),

    /// Unable to read expected bytes from file.
    PartialRead(String),
    /// Unable to write full buffer into file.
    PartialWrite(String),
    /// Returned by disk index or dlog that provide durability support.
    InvalidFile(String),
    /// Thread has failed.
    ThreadFail(String),
    /// On disk snapshot is invalid.
    InvalidSnapshot(String),
    /// Invalid Dlog
    InvalidDlog(String),
    /// Invalid Wal
    InvalidWAL(String),
    /// IO error from std::io
    IoError(String),
    /// Json processing error from jsondata package
    JsonError(jsondata::Error),
    /// String conversion error from std::String, str::str
    Utf8Error(std::str::Utf8Error),
    /// Error converting from one type to another.
    ConversionFail(String),
    /// Return list of files that needs to be purged.
    PurgeFiles(Vec<ffi::OsString>),
    #[doc(hidden)]
    // internal error, given key is less than the entire data set.
    __LessThan,
    // internal error, z-block of robt index has overflowed.
    #[doc(hidden)]
    __ZBlockOverflow(usize),
    // inernal error, m-block of robt index has overflowed.
    #[doc(hidden)]
    __MBlockOverflow(usize),
    // internal error, iteration exhausted in robt index's m-block entries.
    #[doc(hidden)]
    __MBlockExhausted(usize),
    // internal error, iteration exhausted in robt index's z-block entries.
    #[doc(hidden)]
    __ZBlockExhausted(usize),
}

#[macro_export]
macro_rules! err_at {
    ($v:ident, msg:$m:expr) => {
        //
        Err(Error::$v(format!("{}:{} msg:{}", file!(), line!(), $m)))
    };
    ($v:ident, $e:expr) => {
        match $e {
            Ok(val) => Ok(val),
            Err(err) => {
                let msg = format!("{}:{} err:{}", file!(), line!(), err);
                Err(Error::$v(msg))
            }
        }
    };
}

#[macro_export]
macro_rules! parse_at {
    ($e:expr, $t:ty) => {
        match $e.parse::<$t>() {
            Ok(val) => Ok(val),
            Err(err) => {
                let msg = format!("{}:{} parse:{}", file!(), line!(), err);
                Err(Error::ConversionFail(msg))
            }
        }
    };
}

#[macro_export]
macro_rules! convert_at {
    ($e:expr) => {
        match $e.try_into() {
            Ok(val) => Ok(val),
            Err(err) => {
                let msg = format!("{}:{} convert:{}", file!(), line!(), err);
                Err(Error::ConversionFail(msg))
            }
        }
    };
}

#[macro_export]
macro_rules! array_at {
    ($e:expr) => {
        match $e.try_into() {
            Ok(val) => Ok(val),
            Err(err) => {
                let msg = format!("{}:{} {}", file!(), line!(), err);
                Err(Error::ConversionFail(msg))
            }
        }
    };
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        use Error::InvalidCAS;
        use Error::InvalidFile;

        match (self, other) {
            (InvalidCAS(x), InvalidCAS(y)) => x == y,
            (InvalidFile(s1), InvalidFile(s2)) => s1 == s2,
            _ => false,
        }
    }
}
