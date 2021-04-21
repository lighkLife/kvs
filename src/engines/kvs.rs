use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::{fs, io};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write, Seek, SeekFrom, Read};
use std::path::{Path, PathBuf};

use log::{debug, error};

use crate::{KvsError, Result};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use crate::engines::KvsEngine;
use std::sync::{Arc, Mutex};
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use crossbeam_skiplist::SkipMap;


const MERGED_THRESHOLD: u64 = 100;
const INIT_GENERATION: u64 = 0;

/// The `KvStore` stores string key-value pairs.
///
/// Key-value pairs are stored in a `HashMap` in memory and it will be persisted to disk on the future version.
///
/// Example:
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// use kvs::KvsEngine;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct KvStore {
    // directory of file
    path: Arc<PathBuf>,
    // a map of key to command info
    index: Arc<SkipMap<String, CommandInfo>>,
    writer: Arc<Mutex<KvStoreWriter>>,
    reader: KvStoreReader,
}

struct KvStoreWriter {
    // directory of file
    path: Arc<PathBuf>,
    // number of active log file
    write_generation: u64,
    // writer of active log file
    writer: KvsBufWriter<File>,
    // the bytes of invalid command in the log file which would be delete during the next log merge.
    unmerged: u64,
    reader: KvStoreReader,
    // a map of key to command info
    index: Arc<SkipMap<String, CommandInfo>>,
}

struct KvStoreReader {
    path: Arc<PathBuf>,
    // a map of log number to log file reader
    readers: RefCell<BTreeMap<u64, KvsBufReader<File>>>,
    // The newest generation of [`KvWriter`] merged.
    merged_gen: Arc<AtomicU64>,
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        KvStoreReader {
            path: self.path.clone(),
            readers: RefCell::new(BTreeMap::new()),
            merged_gen: self.merged_gen.clone(),
        }
    }
}

impl KvStoreReader {
    fn read_command(&self, cmd_info: CommandInfo) -> Result<Command> {
        self.read_and(cmd_info, |cmd_reader| Ok(serde_json::from_reader(cmd_reader)?))
    }

    fn read_and<F, R>(&self, cmd_info: CommandInfo, fuc: F) -> Result<R>
        where F: FnOnce(io::Take<&mut KvsBufReader<File>>) -> Result<R>
    {
        // delete merged file
        self.close_stale_reader();
        // create reader which not exist in readers
        let mut readers = self.readers.borrow_mut();
        let cur_gen = cmd_info.generation;
        if !readers.contains_key(&cur_gen) {
            let file = File::open(log_file_name(&self.path, cur_gen))?;
            let reader = KvsBufReader::new(file)?;
            readers.insert(cur_gen, reader);
        }
        // read command from file
        let reader = readers.get_mut(&cur_gen).unwrap();
        reader.seek(SeekFrom::Start(cmd_info.pos_start))?;
        let cmd_reader = reader.take(cmd_info.length);
        fuc(cmd_reader)
    }

    fn close_stale_reader(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let generation = *readers.keys().next().unwrap();
            if generation < self.merged_gen.load(Ordering::SeqCst) {
                readers.remove(&generation);
            } else {
                break;
            }
        }
    }
}

impl KvStoreWriter {
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let start_pos = self.writer.pos;
        let cmd = Command::set(key, value);
        serde_json::to_writer(self.writer.by_ref(), &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd_info) = self.index.get(&key) {
                self.unmerged += old_cmd_info.value().length;
            }
            let info = CommandInfo::new(self.write_generation, start_pos, self.writer.pos);
            self.index.insert(key, info);
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
                self.unmerged += old_cmd_info.value().length;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// merge log files to a merged file and delete invalid command
    pub fn merge(&mut self) -> Result<()> {
        debug!("merging");
        // copy valid command to a new log file
        self.write_generation += 1;
        let merged_generation = self.write_generation;
        self.write_generation += 1;
        self.writer = self.create_log_file(self.write_generation)?;

        let mut new_writer = self.create_log_file(merged_generation)?;

        // copy old generation file data to merged_generation file.
        let mut start_pos = 0;
        for entry in self.index.iter() {
            let length = self.reader.read_and(entry.value().clone(), |mut cmd_reader| {
                Ok(io::copy(&mut cmd_reader, &mut new_writer)?)
            })?;
            let cmd_info = CommandInfo::new(merged_generation, start_pos, start_pos + length);
            self.index.insert(entry.key().clone(), cmd_info);
            start_pos += length;
        }
        new_writer.flush()?;
        self.reader.merged_gen.store(merged_generation, Ordering::SeqCst);
        self.reader.close_stale_reader();

        // delete log file which have merged
        let stale_generations = read_generation(&self.path)?
            .into_iter()
            .filter(|&generation| generation < merged_generation);
        for generation in stale_generations {
            let full_path_name = log_file_name(&self.path, generation);
            if let Err(e) = fs::remove_file(&full_path_name) {
                error!("Stale files delete failed: {:?}, {}", full_path_name, e);
            }
        }
        self.unmerged = 0;
        Ok(())
    }

    fn create_log_file(&mut self, generation: u64) -> Result<KvsBufWriter<File>> {
        create_log_file(generation, &self.path)
    }
}

impl KvStore {
    /// Open the KvStore at a given path.
    /// Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;
        let mut index: SkipMap<String, CommandInfo> = SkipMap::new();
        let generation_list = read_generation(&path)?;

        // init reader
        let mut unmerged = 0;
        let mut readers = BTreeMap::new();
        for &generation in &generation_list {
            let path = log_file_name(&path, generation);
            let mut reader = KvsBufReader::new(File::open(&path)?)?;
            unmerged += load_log(generation, &mut reader, &mut index)?;
            readers.insert(generation, KvsBufReader::new(File::open(&path)?)?);
        }

        // open a new log file as the active file for writing logs
        let write_generation = generation_list.iter().max().unwrap_or(&INIT_GENERATION) + 1;
        // init writer
        let writer = create_log_file(write_generation, &path)?;

        let path = Arc::new(path);
        let reader = KvStoreReader {
            path: path.clone(),
            readers: RefCell::new(readers),
            // merge method will set the really newest merged generation for it
            merged_gen: Arc::new(AtomicU64::new(INIT_GENERATION)),
        };
        let index = Arc::new(index);
        let writer = Arc::new(Mutex::new(KvStoreWriter {
            path: path.clone(),
            write_generation,
            writer,
            unmerged,
            reader: reader.clone(),
            index: index.clone(),
        }));

        Ok(KvStore {
            path,
            index,
            writer,
            reader,
        })
    }
}

impl KvsEngine for KvStore {
    /// Get the string value of a string key.
    /// If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(entry) = self.index.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_command(entry.value().clone())? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnknownCommand)
            }
        } else {
            Ok(None)
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

fn create_log_file(
    active_generation: u64,
    path: &Path,
) -> Result<KvsBufWriter<File>> {
    let file_name = log_file_name(path, active_generation);
    let writer = KvsBufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&file_name)?
    )?;
    Ok(writer)
}


fn log_file_name(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("{}.log", generation))
}

fn read_generation(path: &PathBuf) -> Result<Vec<u64>> {
    let generation_list = fs::read_dir(path)?
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
    Ok(generation_list)
}

fn load_log(
    generation: u64,
    reader: &mut KvsBufReader<File>,
    index: &mut SkipMap<String, CommandInfo>,
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
                let info = CommandInfo::new(generation, start_pos, current_pos);
                if let Some(entry) = index.get(&key) {
                    unmerged += entry.value().length;
                }
                index.insert(key, info);
            }
            Command::Remove { key } => {
                if let Some(entry) = index.remove(&key) {
                    unmerged += entry.value().length;
                }
            }
        }
        start_pos = current_pos;
    }
    Ok(unmerged)
}

#[derive(Copy, Clone, Debug)]
struct CommandInfo {
    generation: u64,
    pos_start: u64,
    length: u64,
}

impl CommandInfo {
    fn new(generation: u64, pos_start: u64, pos_stop: u64) -> CommandInfo {
        let length = pos_stop - pos_start;
        CommandInfo {
            generation,
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


