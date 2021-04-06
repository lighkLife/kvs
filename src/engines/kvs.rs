use std::collections::{HashMap, BTreeMap};
use std::ffi::OsStr;
use std::{fs, io};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write, Seek, SeekFrom, Read};
use std::path::{Path, PathBuf};

use crate::{KvsError, Result};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use crate::engines::KvsEngine;


const MERGED_THRESHOLD: u64 = 1024;

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
    // number of active log file
    active_log_number: u64,
    // directory of file
    path: PathBuf,
    // a map of log number to log file reader
    readers: HashMap<u64, KvsBufReader<File>>,
    // writer of active log file
    writer: KvsBufWriter<File>,
    // a map of key to command info
    index: BTreeMap<String, CommandInfo>,
    // the bytes of invalid command in the log file which would be delete during the next log merge.
    unmerged: u64,
}

impl KvStore {
    /// Open the KvStore at a given path.
    /// Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;
        let mut index = BTreeMap::new();
        let log_number_list = read_log_number(&path)?;

        // init reader
        let mut unmerged = 0;
        let mut readers = HashMap::new();
        for &log_number in &log_number_list {
            let path = log_file_name(&path, log_number);
            let mut reader = KvsBufReader::new(File::open(path)?)?;
            unmerged += load_log(log_number, &mut reader, &mut index)?;
            readers.insert(log_number, reader);
        }

        // open a new log file as the active file for writing logs
        let active_log_number = log_number_list.iter().max().unwrap_or(&0) + 1;
        // init writer
        let writer = create_log_file(active_log_number, &path, &mut readers)?;

        Ok(KvStore {
            active_log_number,
            path,
            readers,
            writer,
            index,
            unmerged,
        })
    }


    /// merge log files to a merged file and delete invalid command
    fn merge(&mut self) -> Result<()> {
        // copy valid command to a new log file
        self.active_log_number += 1;
        let new_log_number = self.active_log_number;
        self.active_log_number += 1;
        self.writer = self.create_log_file(self.active_log_number)?;

        let mut new_writer = self.create_log_file(new_log_number)?;

        let mut start_pos = 0;
        for cmd_info in &mut self.index.values_mut() {
            let reader = self.readers.get_mut(&cmd_info.log_number)
                .expect("reader not found");
            if reader.pos != cmd_info.pos_start {
                reader.seek(SeekFrom::Start(cmd_info.pos_start))?;
            }
            let mut cmd_reader = reader.take(cmd_info.length);
            let length = io::copy(&mut cmd_reader, &mut new_writer)?;
            *cmd_info = CommandInfo::new(new_log_number, start_pos, start_pos + length);
            start_pos += length;
        }
        new_writer.flush()?;

        // delete log file which have merged
        let invalid_log_numbers: Vec<u64> = self.readers.keys()
            .filter(|&&log_number| log_number < new_log_number)
            .cloned()
            .collect();
        for log_number in invalid_log_numbers {
            self.readers.remove(&log_number);
            fs::remove_file(log_file_name(&self.path, log_number))?;
        }
        Ok(())
    }

    fn create_log_file(&mut self, log_number: u64) -> Result<KvsBufWriter<File>> {
        create_log_file(log_number, &self.path, &mut self.readers)
    }
}

fn create_log_file(
    active_log_number: u64,
    path: &Path,
    readers: &mut HashMap<u64, KvsBufReader<File>>,
) -> Result<KvsBufWriter<File>> {
    let file_name = log_file_name(path, active_log_number);
    let writer = KvsBufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&file_name)?
    )?;
    readers.insert(active_log_number, KvsBufReader::new(File::open(&file_name)?)?);
    Ok(writer)
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

fn load_log(
    log_number: u64,
    reader: &mut KvsBufReader<File>,
    index: &mut BTreeMap<String, CommandInfo>,
) -> Result<u64> {
    let mut start_pos = reader.seek(SeekFrom::Start(0))?;
    let reader = reader.reader.get_mut();
    let mut stream = Deserializer::from_reader(reader)
        .into_iter::<Command>();

    let mut unmerged = 0;
    while let Some(cmd) = stream.next() {
        let current_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                let info = CommandInfo::new(log_number, start_pos, current_pos);
                if let Some(old_cmd_info) = index.insert(key, info) {
                    unmerged += old_cmd_info.length;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd_info) = index.remove(&key) {
                    unmerged += old_cmd_info.length;
                }
            }
        }
        start_pos = current_pos;
    }
    Ok(unmerged)
}

struct CommandInfo {
    log_number: u64,
    pos_start: u64,
    length: u64,
}

impl CommandInfo {
    fn new(log_number: u64, pos_start: u64, pos_stop: u64) -> CommandInfo {
        let length = pos_stop - pos_start;
        CommandInfo {
            log_number,
            pos_start,
            length,
        }
    }
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


struct KvsBufReader<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

struct KvsBufWriter<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<R: Read + Seek> KvsBufReader<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(KvsBufReader {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for KvsBufReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let length = self.reader.read(buf)?;
        self.pos += length as u64;
        Ok(length)
    }
}

impl<R: Read + Seek> Seek for KvsBufReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

impl<W: Write + Seek> KvsBufWriter<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(KvsBufWriter {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for KvsBufWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let length = self.writer.write(buf)?;
        self.pos += length as u64;
        Ok(length)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for KvsBufWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}


impl KvsEngine for KvStore {
    /// Get the string value of a string key.
    /// If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_info) = self.index.get(&key) {
            let reader = self.readers.get_mut(&cmd_info.log_number)
                .expect("reader not found");
            reader.seek(SeekFrom::Start(cmd_info.pos_start))?;
            let log_reader = reader.take(cmd_info.length);
            if let Command::Set { value, .. } = serde_json::from_reader(log_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnknownCommand)
            }
        } else {
            Ok(None)
        }
    }
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let start_pos = self.writer.pos;
        let cmd = Command::set(key, value);
        serde_json::to_writer(self.writer.by_ref(), &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            let current_pos = self.writer.pos;
            let info = CommandInfo::new(self.active_log_number, start_pos, current_pos);
            if let Some(old_cmd_info) = self.index.insert(key, info) {
                self.unmerged += old_cmd_info.length;
            }
        }
        if self.unmerged > MERGED_THRESHOLD {
            self.merge()?;
        }
        Ok(())
    }


    /// Remove a given key.
    /// Return an error if the key does not exist or is not removed successfully.
    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(self.writer.by_ref(), &cmd)?;
            self.writer.flush()?;
            if let Command::Remove { key } = cmd {
                let old_cmd_info = self.index.remove(&key)
                    .expect("Key not found");
                self.unmerged += old_cmd_info.length;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}


