use sled::Db;
use crate::engines::KvsEngine;
use crate::{Result, KvsError};

/// sled ksv engine
#[derive(Clone)]
pub struct SledKvsEngine {
    engine: Db,
}

impl SledKvsEngine {
    /// create a SledKvsEngine instance
    pub fn new(engine: Db) -> Result<Self> {
        Ok(SledKvsEngine { engine })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        let value = self.engine.get(key)?;
        Ok(value
            .map(|i_vec| AsRef::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?
        )
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.engine.insert(key, value.into_bytes()).map(|_| ())?;
        self.engine.flush()?;
        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        self.engine.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.engine.flush()?;
        Ok(())
    }
}