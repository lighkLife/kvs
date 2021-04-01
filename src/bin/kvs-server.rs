use clap::arg_enum;
use structopt::StructOpt;
use std::net::SocketAddr;
use std::fmt::{Display, Formatter};

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
    let opt = Opt::from_args() as Opt;
    println!("{:}", opt.addr);
    if let Some(engine) = opt.engine {
        println!("{:}", engine);
    }
}