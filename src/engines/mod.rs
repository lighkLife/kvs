use crate::Result;

/// Trait for a key value storage engine
pub trait KvsEngine {
    /// Get the value of key
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Set the value of key
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Remove the value-key pair.
    fn remove(&mut self, key: String) -> Result<()>;
}

mod sled;
mod kvs;

pub use self::sled::SledKvsEngine;
pub use self::kvs::KvStore;
