use failure::Fail;
use std::io;
use std::io::Error;

/// kvs error
#[derive(Debug, Fail)]
pub enum KvsError {
    /// IO error
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// Serde serialization or deserialization error
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    /// Remove a not exit key error
    #[fail(display = "Key not found")]
    KeyNotFound,
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

/// kvs result
pub type Result<T> = std::result::Result<T, KvsError>;