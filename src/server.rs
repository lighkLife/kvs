use std::net::{ToSocketAddrs, TcpListener, TcpStream};
use crate::err::Result;
use crate::KvsError;
use crate::protocol::*;
use log::{debug, error};
use std::io::{BufReader, BufWriter, Write};
use crate::engines::KvsEngine;

/// struct server
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    /// crate a kvs server instance
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    /// Start kvs server
    pub fn start<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Err(e) => error!("Connection failed: {}", e),
                Ok(stream) => {
                    if let Err(e) = self.handle_client(stream) {
                        error!("Handle client stream failed: {}", e);
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_client(&mut self, mut stream: TcpStream) -> Result<()> {
        let peer = stream.peer_addr()?;
        debug!("Connection established from {}", &peer);
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let deserializer_iter = serde_json::Deserializer::from_reader(reader)
            .into_iter::<KvsRequest>();
        for request in deserializer_iter {
            let request = request?;
            debug!("recv from {}: {:?}", &peer, &request);
            match request {
                KvsRequest::Get { key } => {
                    let value = self.engine.get(key)?;
                    let response = GetResponse::Ok(value);
                    serde_json::to_writer(&mut writer, &response)?;
                    writer.flush()?;
                    debug!("resp to   {}: {:?}", &peer, &response);
                }
                KvsRequest::Set { key, value } => {
                    let value = self.engine.set(key, value)?;
                    let response = SetResponse::Ok(value);
                    serde_json::to_writer(&mut writer, &response)?;
                    writer.flush()?;
                    debug!("resp to   {}: {:?}", &peer, &response);
                }
                KvsRequest::Remove { key } => {
                    let value = self.engine.remove(key)?;
                    let response = SetResponse::Ok(value);
                    serde_json::to_writer(&mut writer, &response)?;
                    writer.flush()?;
                    debug!("resp to   {}: {:?}", &peer, &response);
                }
            };
        }
        Ok(())
    }
}

