use std::collections::{HashMap, BTreeMap};
use std::ffi::OsStr;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write, Seek, SeekFrom, Read};
use std::path::{Path, PathBuf};

use crate::{KvsError, Result};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::env::current_dir;

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
    reader: BufReader<File>,
    writer: BufWriter<File>,
    store: BTreeMap<String, String>,
}

impl KvStore {
    /// Open the KvStore at a given path.
    /// Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;

        let mut store = BTreeMap::new();

        // 日志文件序号从1开始
        let log_number_list = read_log_number(&path)?;
        let max_log_number = log_number_list.last().unwrap_or(&1);
        let file_name = log_file_name(&path, *max_log_number);
        let mut write_options = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&file_name)?;
        let mut writer = BufWriter::new(write_options);
        let mut read_file = File::open(&file_name)?;
        let mut reader = BufReader::new(read_file);

        load_log(&mut reader, &mut store);
        Ok(KvStore {
            path,
            reader,
            writer,
            store,
        })
    }


    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        serde_json::to_writer( self.writer.by_ref(), &cmd)?;
        self.writer.flush()?;
        if let Command::Set {key, value} = cmd {
            self.store.insert(key, value);
        }
        Ok(())
    }

    /// Get the string value of a string key.
    /// If the key does not exist, return None. Return an error if the value is not read successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(value) = self.store.get(&key) {
            Ok(Some(String::from(value)))
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    /// Return an error if the key does not exist or is not removed successfully.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.store.contains_key(&key){
            let cmd = Command::remove(key);
            serde_json::to_writer(self.writer.by_ref(), &cmd)?;
            self.writer.flush()?;
            if let Command::Remove { key } = cmd {
                self.store.remove(&key);
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
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

fn load_log(reader: &mut BufReader<File>, store: &mut BTreeMap<String,String>) -> Result<()> {
    let reader = reader.get_mut();
    let mut stream = Deserializer::from_reader(reader)
        .into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        match cmd? {
            Command::Set { key, value } => {
                store.insert(key, value);
            }
            Command::Remove { key } => {
                store.remove(&key);
            }
        }
    }
    Ok(())
}

struct CommandInfo{
    log_number: u64,
    pos: u64,
    length: u64,
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
        Command::Remove { key }
    }
}