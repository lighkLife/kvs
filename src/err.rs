use failure::Fail;
use std::io;
use std::io::Error;
use std::string::FromUtf8Error;

/// kvs error
#[derive(Fail, Debug)]
pub enum KvsError {
    /// IO error
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// Serde serialization or deserialization error
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    /// Sled error
    #[fail(display = "sled error: {}", _0)]
    Sled(#[cause] sled::Error),
    /// Converting a `String` from a UTF-8 byte vector error
    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] FromUtf8Error),
    /// Remove a not exit key error
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// Server config is invalid error.
    #[fail(display = "Server start failed.")]
    ServerStart,
    /// Client send a invalid request to server.
    #[fail(display = "{}", _0)]
    InvalidOperation(String),
    /// Unknown command
    #[fail(display = "Unknown command")]
    UnknownCommand,
}


impl From<io::Error> for KvsError {
    fn from(err: Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::Sled(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::Utf8(err)
    }
}

/// kvs result
pub type Result<T> = std::result::Result<T, KvsError>;