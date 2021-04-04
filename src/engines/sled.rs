use sled::Db;
use crate::engines::KvsEngine;
use std::path::Path;
use crate::{Result, KvsError};

/// sled ksv engine
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    /// create a SledKvsEngine instance
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(SledKvsEngine { db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let value = self.db.get(key)?;
        Ok(value
            .map(|i_vec| AsRef::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?
        )
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes()).map(|_| ())?;
        self.db.flush()?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.db.flush()?;
        Ok(())
    }
}