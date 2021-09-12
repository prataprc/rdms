//! Module error define enumeration of all `rdms` errors.
//!
//! Convertion traits, from other error types, like from std-lib, to `rdms`
//! error are implemented in this module.

use std::{error, fmt, result};

/// Error enumerates over all possible errors cases in `rdms` package.
pub enum Error {
    /// Fatal failure, caller can treat this as sever as a panic!()
    Fatal(String, String),
    /// API / function not supported
    NotImplemented(String, String),
    /// Error because the value was not initialized as expected.
    UnInitialized(String, String),
    /// TimeFail, std time related API failed.
    TimeFail(String, String),
    /// Inter-Process-Communication error from std::mpsc
    IPCFail(String, String),
    /// System level failure.
    SystemFail(String, String),
    /// Invalid input from application, like function arguments,
    /// file input etc..
    InvalidInput(String, String),
    /// API is being misused, as in there are not invoked in
    /// suggested order/manner.
    APIMisuse(String, String),
    /// De-serialization failed.
    DecodeFail(String, String),
    /// Returned by disk index or dlog that provide durability support.
    InvalidFile(String, String),
    /// Error converting from one type to another.
    ConversionFail(String, String),
    /// IO error from std::io
    IoError(String, String),

    /// Supplied key is not found in the index.
    KeyNotFound(String, String),
    /// Index is empty
    EmptyIndex(String, String),
    /// Can be returned by set_cas() API when:
    /// * In non-lsm mode, requested entry is missing but specified
    ///   CAS is not ZERO. Note that this combination is an alias for
    ///   create-only operation.
    /// * In lsm mode, requested entry is marked as deleted, and
    ///   specifed CAS is neither ZERO, nor matching with entry's
    ///   last modified sequence-number.
    /// * Requested entry's last modified sequence-number does not
    ///   match with specified CAS.
    InvalidCAS(String, String),
    /// Key size, after serializing, has exceeded the configured,
    /// (or hard coded) limit.
    KeySizeExceeded(String, String),
    /// Value size, after serializing, has exceed the configured,
    /// (or hard coded) limit.
    ValueSizeExceeded(String, String),
    /// Value-diff size, after serializing, exceeds limit.
    DiffSizeExceeded(String, String),
    /// Return list of files that needs to be purged.
    PurgeFiles(String, String),

    #[doc(hidden)]
    // internal error, given key is less than the entire data set.
    __LessThan(String, String),
    // internal error, z-block of robt index has overflowed.
    #[doc(hidden)]
    __ZBlockOverflow(String, String),
    // inernal error, m-block of robt index has overflowed.
    #[doc(hidden)]
    __MBlockOverflow(String, String),
    // internal error, iteration exhausted in robt index's m-block entries.
    #[doc(hidden)]
    __MBlockExhausted(String, String),
    // internal error, iteration exhausted in robt index's z-block entries.
    #[doc(hidden)]
    __ZBlockExhausted(String, String),
}

/// Short form to compose Error values.
///
/// Here are few possible ways:
///
/// ```ignore
/// use crate::Error;
/// err_at!(ParseError, msg: format!("bad argument"));
/// ```
///
/// ```ignore
/// use crate::Error;
/// err_at!(ParseError, std::io::read(buf));
/// ```
///
/// ```ignore
/// use crate::Error;
/// err_at!(ParseError, std::fs::read(file_path), format!("read failed"));
/// ```
///
#[macro_export]
macro_rules! err_at {
    ($v:ident, msg: $($arg:expr),+) => {{
        let prefix = format!("{}:{}", file!(), line!());
        Err(Error::$v(prefix, format!($($arg),+)))
    }};
    ($v:ident, $e:expr) => {{
        match $e {
            Ok(val) => Ok(val),
            Err(err) => {
                let prefix = format!("{}:{}", file!(), line!());
                Err(Error::$v(prefix, format!("{}", err)))
            }
        }
    }};
    ($v:ident, $e:expr, $($arg:expr),+) => {{
        match $e {
            Ok(val) => Ok(val),
            Err(err) => {
                let prefix = format!("{}:{}", file!(), line!());
                let msg = format!($($arg),+);
                Err(Error::$v(prefix, format!("{} {}", err, msg)))
            }
        }
    }};
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Error::Fatal(p, m) => write!(f, "{} Fatal:{}", p, m),
            Error::NotImplemented(p, m) => write!(f, "{} NotImplemented:{}", p, m),
            Error::UnInitialized(p, m) => write!(f, "{} UnInitialized:{}", p, m),
            Error::TimeFail(p, m) => write!(f, "{} TimeFail:{}", p, m),
            Error::IPCFail(p, m) => write!(f, "{} IPCFail:{}", p, m),
            Error::SystemFail(p, m) => write!(f, "{} SystemFail:{}", p, m),
            Error::InvalidInput(p, m) => write!(f, "{} InvalidInput:{}", p, m),
            Error::APIMisuse(p, m) => write!(f, "{} APIMisuse:{}", p, m),
            Error::DecodeFail(p, m) => write!(f, "{} DecodeFail:{}", p, m),
            Error::InvalidFile(p, m) => write!(f, "{} InvalidFile:{}", p, m),
            Error::ConversionFail(p, m) => write!(f, "{} ConversionFail:{}", p, m),
            Error::IoError(p, m) => write!(f, "{} IoError:{}", p, m),
            Error::KeyNotFound(p, m) => write!(f, "{} KeyNotFound:{}", p, m),
            Error::EmptyIndex(p, m) => write!(f, "{} EmptyIndex:{}", p, m),
            Error::InvalidCAS(p, m) => write!(f, "{} InvalidCAS:{}", p, m),
            Error::KeySizeExceeded(p, m) => write!(f, "{} KeySizeExceeded:{}", p, m),
            Error::ValueSizeExceeded(p, m) => write!(f, "{} ValueSizeExceeded:{}", p, m),
            Error::DiffSizeExceeded(p, m) => write!(f, "{} DiffSizeExceeded:{}", p, m),
            Error::PurgeFiles(p, m) => write!(f, "{} PurgeFiles:{}", p, m),
            Error::__LessThan(p, m) => write!(f, "{} __LessThan:{}", p, m),
            Error::__ZBlockOverflow(p, m) => write!(f, "{} __ZBlockOverflow:{}", p, m),
            Error::__MBlockOverflow(p, m) => write!(f, "{} __MBlockOverflow:{}", p, m),
            Error::__MBlockExhausted(p, m) => write!(f, "{} __MBlockExhausted:{}", p, m),
            Error::__ZBlockExhausted(p, m) => write!(f, "{} __ZBlockExhausted:{}", p, m),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self)
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        use Error::InvalidCAS;
        use Error::InvalidFile;

        match (self, other) {
            (InvalidCAS(_, a), InvalidCAS(_, b)) => a == b,
            (InvalidFile(_, a), InvalidFile(_, b)) => a == b,
            _ => false,
        }
    }
}

impl error::Error for Error {}

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

#[macro_export]
macro_rules! parse_at {
    ($e:expr, $t:ty) => {
        match $e.parse::<$t>() {
            Ok(val) => Ok(val),
            Err(err) => {
                let msg = format!("{}:{} parse: {}", file!(), line!(), err);
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
                let msg = format!("{}:{} {:?} convert: {}", file!(), line!(), $e, err);
                Err(Error::ConversionFail(msg))
            }
        }
    };
}
