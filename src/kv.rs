use std::collections::HashMap;
use std::path::{PathBuf, Path};
use crate::{KvsError, Result};
use std::fs::File;
use std::io::{BufReader, Seek, Read, Write, BufWriter, SeekFrom};

/// The `KvStore` stores string key-value pairs.
///
/// Key-value pairs are stored in a `HashMap` in memory and it will be persisted to disk on the future version.
///
/// Example:
/// ```rust
/// # use kvs::KvStore;
/// let mut kvs = KvStore::new();
/// kvs.set("key".to_owned(), "value".to_owned());
/// assert_eq!(kvs.get("key".to_owned()), Some("value".to_owned()));
/// kvs.remove("key".to_owned());
/// assert_eq!(kvs.get("key".to_owned()), None);
/// ```
pub struct KvStore {
    path: PathBuf,
    reader: BufReaderWithPos<File>,
    // writer: BufWriterWithPos<File>,
    log_number: u32,
}

impl KvStore {
    /// Open the KvStore at a given path.
    /// Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;

        let log_number = 0;
        let file_name = file + log_number + ".log";
        let mut file = File::open(Path::new(&file_name))?;
        let mut buf_reader = BufReader::new(file);
        // let mut writer = BufReader::new(file);
        let mut reader = BufReaderWithPos {}


        Ok(KvStore {
            path,
            // writer,
            log_number,
        })
    }


    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // self.storage.insert(key, value);
        Ok(())
    }

    /// Get the string value of a string key.
    /// If the key does not exist, return None. Return an error if the value is not read successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // let value = self.storage.get(&key).cloned();
        Ok(Some("".to_owned()))
    }

    /// Remove a given key.
    /// Return an error if the key does not exist or is not removed successfully.
    pub fn remove(&mut self, key: String) -> Result<()> {
        // self.storage.remove(&key);
        Ok(())
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

struct BufWriterWithPos<W: Write + Seel> {
    writer: BufWriter<W>,
    pos: u64,
}