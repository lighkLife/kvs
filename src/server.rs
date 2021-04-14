use std::net::{ToSocketAddrs, TcpListener, TcpStream};
use crate::err::Result;
use crate::protocol::*;
use log::{debug, error};
use std::io::{BufReader, BufWriter, Write};
use crate::engines::KvsEngine;
use crate::thread_pool::{ThreadPool};

/// struct server
pub struct KvServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvServer<E> {
    /// crate a kvs server instance
    pub fn new(engine: E) -> Self {
        KvServer { engine }
    }

    /// Start kvs server
    pub fn start<A: ToSocketAddrs, P: ThreadPool>(self, addr: A, pool: P) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            pool.spawn(move || match stream {
                Err(e) => error!("Connection failed: {}", e),
                Ok(stream) => {
                    if let Err(e) = handle_client(engine, stream) {
                        error!("Handle client stream failed: {}", e);
                    }
                }
            })
        }
        Ok(())
    }
}

fn handle_client<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
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
                let response = match engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(format!("{}", e)),
                };
                serde_json::to_writer(&mut writer, &response)?;
                writer.flush()?;
                debug!("resp to   {}: {:?}", &peer, &response);
            }
            KvsRequest::Set { key, value } => {
                let response = match engine.set(key, value) {
                    Ok(value) => SetResponse::Ok(value),
                    Err(e) => SetResponse::Err(format!("{}", e)),
                };
                serde_json::to_writer(&mut writer, &response)?;
                writer.flush()?;
                debug!("resp to   {}: {:?}", &peer, &response);
            }
            KvsRequest::Remove { key } => {
                let response = match engine.remove(key) {
                    Ok(value) => RemoveResponse::Ok(value),
                    Err(e) => RemoveResponse::Err(format!("{}", e)),
                };
                serde_json::to_writer(&mut writer, &response)?;
                writer.flush()?;
                debug!("resp to   {}: {:?}", &peer, &response);
            }
        };
    }
    Ok(())
}

