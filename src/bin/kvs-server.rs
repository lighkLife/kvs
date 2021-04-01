use clap::arg_enum;
use structopt::StructOpt;
use std::net::SocketAddr;
use std::fmt::{Display, Formatter};
use log::{error, info, warn, debug};
use log::LevelFilter;
use std::env::current_dir;
use kvs::*;
use std::fs;
use std::process::exit;


const DEFAULT_ADDR: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;
const ENGINE_FILE_NAME: &str = "engine";

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
    help = "Set storage engines, either kvs or sled.",
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
    env_logger::builder().filter_level(LevelFilter::Debug).init();
    let mut opt = Opt::from_args() as Opt;
    let result = previous_engine()
        .and_then(|previous_engine| {
            if opt.engine.is_none() {
                opt.engine = previous_engine;
            }
            debug!("engine: current={:?}, previous={:?}", opt.engine, previous_engine);

            if previous_engine.is_some() && previous_engine != opt.engine {
                error!("The storage engine {} has been set up and cannot be replaced",
                       previous_engine.unwrap());
                exit(1);
            }
            let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);
            info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
            info!("listening on {}", opt.addr);
            info!("use {} engines", engine);

            //save engine type.
            fs::write(current_dir()?.join(ENGINE_FILE_NAME), format!("{}", engine))?;
            Ok(())
        });
    if let Err(e) = result {
        error!("{}", e);
        exit(1);
    }
}

fn previous_engine() -> Result<Option<Engine>> {
    let engine_path = current_dir()?.join(ENGINE_FILE_NAME);
    if !engine_path.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine_path)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            error!("Invalid engines: {}", e);
            Err(KvsError::ServerStart)
        }
    }
}
