use failure::Fail;
use std::{io, string};

/// well-defined Result
pub type Result<T> = std::result::Result<T, KVStoreError>;

#[derive(Fail, Debug)]
/// well-defined Error
pub enum KVStoreError {
    /// Io error
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    /// Serde error
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),

    /// Sled error
    #[fail(display = "{}", _0)]
    Sled(#[cause] sled::Error),

    /// FromUtf8 Error
    #[fail(display = "Utf8 Error {}", _0)]
    Utf8Error(#[cause] string::FromUtf8Error),

    /// Key not found error
    #[fail(display = "Key not found")]
    KeyNotFound,

    /// Unknown command type error
    #[fail(display = "Unknown command type")]
    UnknownCommandType,

    /// Unknown engine type error
    #[fail(display = "Unknown engine type")]
    UnknownEngineType,

    /// Unknown engine type error
    #[fail(display = "Change engine after initialization")]
    ChangeEngineError,

    /// common string error
    #[fail(display = "{}", _0)]
    CommonStringError(String),
}

impl From<io::Error> for KVStoreError {
    fn from(err: io::Error) -> Self {
        KVStoreError::Io(err)
    }
}

impl From<serde_json::Error> for KVStoreError {
    fn from(err: serde_json::Error) -> Self {
        KVStoreError::Serde(err)
    }
}

impl From<sled::Error> for KVStoreError {
    fn from(err: sled::Error) -> Self {
        KVStoreError::Sled(err)
    }
}

impl From<string::FromUtf8Error> for KVStoreError {
    fn from(err: string::FromUtf8Error) -> Self {
        KVStoreError::Utf8Error(err)
    }
}
