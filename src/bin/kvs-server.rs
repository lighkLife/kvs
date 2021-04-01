use clap::arg_enum;
use structopt::StructOpt;
use std::net::SocketAddr;
use std::fmt::{Display, Formatter};
use log::{error, info, warn};
use log::LevelFilter;
use std::env::current_dir;
use kvs::{Result, KvsError};
use std::fs;


const DEFAULT_ADDR: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server", about = "A key-value storage server.")]
struct Opt {
    #[structopt(
    long,
    default_value = DEFAULT_ADDR,
    help = "Set ip address and port number with the format IP:PORT.",
    parse(try_from_str),
    value_name = "IP:PORT",
    )]
    addr: SocketAddr,
    #[structopt(
    long,
    help = "Set storage engine, either kvs or sled.",
    possible_values = & Engine::variants(),
    value_name = "ENGINE-NAME",
    )]
    engine: Option<Engine>,
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum Engine {
        kvs,
        sled,
    }
}

fn main() {
    env_logger::builder().init();
    let opt = Opt::from_args() as Opt;
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("listening on {}", opt.addr);
    info!("use {} engine", opt.engine.unwrap_or(DEFAULT_ENGINE));

    let mut engine;
}

fn previous_engine() -> Result<Option<Engine>> {
    let engine_path = current_dir()?.join("engine");
    if !engine_path.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine_path)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            error!("Invalid engine: {}", e);
            KvsError::InvalidConfig
        }
    }
}
