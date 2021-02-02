use failure::Fail;
use std::io;
use std::io::Error;

/// kvs error
#[derive(Debug, Fail)]
pub enum KvsError {
    /// IO error
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// Unknown command
    #[fail(display = "Unknown command")]
    UnknownCommand,
}


impl From<io::Error> for KvsError {
    fn from(err: Error) -> KvsError {
        KvsError::Io(err)
    }
}

/// kvs result
pub type Result<T> = std::result::Result<T, KvsError>;