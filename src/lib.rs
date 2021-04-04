#![deny(missing_docs)]
//! A simple key-value storage.
mod kv;
mod err;
mod protocol;
mod client;
mod server;
mod engines;

pub use kv::KvStore;
pub use err::{Result, KvsError};
pub use client::KvsClient;
pub use server::KvsServer;
