#![deny(missing_docs)]
//! A simple key-value storage.
mod kv;
mod err;

pub use kv::KvStore;
pub use err::{Result, KvsError};
