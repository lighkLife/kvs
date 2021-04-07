use serde_json::de::Deserializer;
use serde_json::de::{IoRead};
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};
use crate::{KvsError, Result};
use crate::protocol::{GetResponse, SetResponse, RemoveResponse, KvsRequest};
use serde::Deserialize;

/// Kvs Client.
pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    /// connect to kvs server
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let reader_stream = TcpStream::connect(addr)?;
        let writer_stream = reader_stream.try_clone()?;
        Ok(KvsClient {
            reader: Deserializer::from_reader(BufReader::new(reader_stream)),
            writer: BufWriter::new(writer_stream),
        })
    }

    /// get value of key from server
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &KvsRequest::Get { key })?;
        self.writer.flush()?;
        let response = GetResponse::deserialize(&mut self.reader)?;
        match response {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(msg) => Err(KvsError::InvalidOperation(msg)),
        }
    }

    /// set value for key to server
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &KvsRequest::Set { key, value })?;
        self.writer.flush()?;
        let response = SetResponse::deserialize(&mut self.reader)?;
        match response {
            SetResponse::Ok(()) => Ok(()),
            SetResponse::Err(msg) => Err(KvsError::InvalidOperation(msg)),
        }
    }

    /// remove key and value from server
    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &KvsRequest::Remove { key })?;
        self.writer.flush()?;
        let response = RemoveResponse::deserialize(&mut self.reader)?;
        match response {
            RemoveResponse::Ok(()) => Ok(()),
            RemoveResponse::Err(msg) => Err(KvsError::InvalidOperation(msg)),
        }
    }
}