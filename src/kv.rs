use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::{KvsError, Result};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

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
    max_log_number: u64,
    reader_map: HashMap<u64, BufReader<File>>,
    writer: BufWriter<File>,
}

impl KvStore {
    /// Open the KvStore at a given path.
    /// Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;

        let log_number_list = read_log_number(&path)?;
        let mut reader_map = HashMap::new();
        for &log_number in &log_number_list {
            let file = File::open(log_file_name(&path, log_number))?;
            let mut reader = BufReader::new(file);
            reader_map.insert(log_number, reader);
        }

        // 开始运行时，打开一个新文件来写入，后面自动合并
        let mut max_log_number = log_number_list.last().unwrap_or(&0) + 1;
        let write_file_name = log_file_name(&path, max_log_number);
        let mut write_file = File::open(Path::new(&write_file_name))?;
        let mut writer = BufWriter::new(write_file);
        Ok(KvStore {
            path,
            max_log_number,
            reader_map,
            writer,
        })
    }


    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // self.storage.insert(key, value);
        let command = Command::set(key, value);
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;
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

fn log_file_name(dir: &Path, log_number: u64) -> PathBuf {
    dir.join(format!("{}.log", log_number))
}

fn read_log_number(path: &PathBuf) -> Result<Vec<u64>> {
    let log_number_list = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    Ok(log_number_list)
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::remove(key)
    }
}