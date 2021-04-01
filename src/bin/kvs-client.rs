use std::net::{SocketAddr, TcpStream};
use structopt::StructOpt;
use std::io::{BufReader, BufWriter, Write, Read};
use kvs::*;
const DEFAULT_ADDR: &str = "127.0.0.1:4000";


#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-client", about = "A client for kvs server.")]
struct Opt {
    #[structopt(subcommand)]
    cmd: Cmd,
}


#[derive(Debug, StructOpt)]
enum Cmd {
    #[structopt(about = "Set the value of a string key to a string.")]
    Set {
        #[structopt(value_name = "KEY", help = "A string key")]
        key: String,
        #[structopt(value_name = "VALUE", help = "A string value of the key.")]
        value: String,
        #[structopt(
        long,
        help = "Set ip address and port number with the format IP:PORT.",
        value_name = "IP:PORT",
        default_value = DEFAULT_ADDR,
        parse(try_from_str),
        )]
        addr: SocketAddr,
    },

    #[structopt(about = "Get the string value of a given string key.")]
    Get {
        #[structopt(value_name = "KEY", help = "A string key")]
        key: String,
        #[structopt(
        long,
        help = "Set ip address and port number with the format IP:PORT.",
        value_name = "IP:PORT",
        default_value = DEFAULT_ADDR,
        parse(try_from_str),
        )]
        addr: SocketAddr,
    },

    #[structopt(about = "Remove a given key.")]
    Rm {
        #[structopt(value_name = "KEY", help = "A string key")]
        key: String,
        #[structopt(
        long,
        help = "Set ip address and port number with the format IP:PORT.",
        value_name = "IP:PORT",
        default_value = DEFAULT_ADDR,
        parse(try_from_str),
        )]
        addr: SocketAddr,
    },
}

fn main(){
    let opt = Opt::from_args() as Opt;
    match opt.cmd {
        Cmd::Get { key, addr } => {
            let writer_stream = TcpStream::connect(addr).unwrap();
            let reader_stream = writer_stream.try_clone().unwrap();

            let mut writer = BufWriter::new(writer_stream);
            writer.write_fmt(format_args!("Get {}", key));
            writer.flush();

            let mut reader = BufReader::new(reader_stream);
            let mut buffer = String::new();
            reader.read_to_string(&mut buffer);
            println!("response:{}", buffer);
        }
        Cmd::Set { key, value, addr } => {}
        Cmd::Rm { key, addr } => {}
    }
}