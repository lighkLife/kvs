use std::collections::{HashMap, BTreeMap};
use std::ffi::OsStr;
use std::{fs, io};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write, Seek, SeekFrom, Read};
use std::path::{Path, PathBuf};

use log::{debug};

use crate::{KvsError, Result};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use crate::engines::KvsEngine;
use std::sync::{Arc, Mutex};
use std::cell::{RefCell, RefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use skiplist::SkipMap;


const MERGED_THRESHOLD: u64 = 1024;
const INIT_GENERATION: u64 = 0;

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
        self.read_and(cmd_info, |reader| serde_json::from_reader(reader)?)
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
            let mut file = File::open(log_file_name(&self.path, cur_gen))?;
            let reader = KvsBufReader::new(file)?;
            readers.insert(cur_gen, reader);
        }
        // read command from file
        let reader = readers.get_mut(&cur_gen).unwrap();
        reader.seek(SeekFrom::Start(cmd_info.pos_start));
        reader.take(cmd_info.length);
        fuc(reader)
    }

    fn close_stale_reader(&self) {
        let mut readers = self.readers.borrow_mut();
        for &generation in readers.keys() {
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
            let current_pos = self.writer.pos;
            let info = CommandInfo::new(self.write_generation, start_pos, current_pos);
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
        for (key, command_info) in self.index.iter() {
            let length = self.reader.read_and(*command_info.value(), |mut cmd_reader| {
                Ok(io::copy(&mut cmd_reader, &mut new_writer)?)
            })?;
            let cmd_info = CommandInfo::new(merged_generation, start_pos, start_pos + length);
            self.index.insert(key.clone(), cmd_info);
            start_pos += length;
        }
        new_writer.flush()?;
        self.reader.merged_gen.store(merged_generation, Ordering::SeqCst);
        self.reader.close_stale_reader();

        // delete log file which have merged
        let stale_generations: Vec<u64> = self.readers.keys()
            .filter(|&&generation| generation < merged_generation)
            .cloned()
            .collect();
        for generation in stale_generations {
            self.readers.remove(&generation);
            fs::remove_file(log_file_name(&self.path, generation))?;
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
            write_generation,
            writer,
            unmerged,
            reader,
            index,
        }));

        Ok(KvStore {
            path,
            index: index.clone(),
            writer,
            reader: reader.clone(),
        })
    }
}

impl KvsEngine for KvStore {
    /// Get the string value of a string key.
    /// If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(&cmd_info) = self.index.get(&key) {
            if let Command::Set { value, .. } = self.reader.read(cmd_info) {
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


