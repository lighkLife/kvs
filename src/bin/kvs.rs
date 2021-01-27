extern crate clap;

use clap::{App, Arg, SubCommand};
use std::process::exit;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("KEY"))
                .arg(Arg::with_name("VALUE"))
                .help("save key value pair to kvs.")
        )
        .subcommand(
            SubCommand::with_name("get")
                .arg(Arg::with_name("KEY"))
                .help("Get the value of key.")
        )
        .subcommand(
            SubCommand::with_name("rm")
                .arg(Arg::with_name("KEY"))
                .help("Remove the value of key.")
        )
        .get_matches();

    match matches.subcommand() {
        ("set", _) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("get", _) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("rm", _) => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => unreachable!(),
    }
}
