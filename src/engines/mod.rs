use crate::Result;

/// Trait for a key value storage engine
pub trait KvsEngine: Clone + Send + 'static {
    /// Get the value of key
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Set the value of key
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Remove the value-key pair.
    fn remove(&self, key: String) -> Result<()>;
}

mod sled;
mod kvs;

pub use self::sled::SledKvsEngine;
pub use self::kvs::{KvsStoreEngine, KvStore};
