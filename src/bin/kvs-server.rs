use clap::arg_enum;
use structopt::StructOpt;
use std::net::SocketAddr;
use log::{error, info, debug};
use log::LevelFilter;
use std::env::current_dir;
use kvs::*;
use std::fs;
use std::process::exit;
use kvs::thread_pool::{ThreadPool, RayonThreadPool};

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
    help = "Set storage engines, either kvs or sled. Default kvs.",
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

            let pool = RayonThreadPool::new(num_cpus::get() as u32)?;
            let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);
            info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
            info!("listening on {}", opt.addr);
            info!("use {} engines", engine);

            //save engine type.
            fs::write(current_dir()?.join(ENGINE_FILE_NAME), format!("{}", engine))?;
            match engine {
                Engine::kvs => {
                    let store = KvStore::open(current_dir()?)?;
                    let engine = KvsStoreEngine::new(store);
                    start_server(&mut opt, engine, pool)?;
                }
                Engine::sled => {
                    let db = sled::open(current_dir()?)?;
                    let engine = SledKvsEngine::new(db)?;
                    start_server(&mut opt, engine, pool)?;
                }
            };
            Ok(())
        });
    if let Err(e) = result {
        error!("{}", e);
        exit(1);
    }
}

fn start_server<E: KvsEngine, P: ThreadPool>(opt: &mut Opt, engine: E, pool: P) -> Result<()> {
    let server = KvServer::new(engine);
    server.start(opt.addr, pool)?;
    Ok(())
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
