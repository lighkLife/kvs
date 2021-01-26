extern crate clap;

use clap::{App, Arg};

fn main() {
    let matches = App::new("kvs")
        .version("0.1.0")
        .author("lighk")
        .about("key value storage")
        .arg(
            Arg::with_name("V")
                .help("Show the version of kvs.")
        )
        .arg(
            Arg::with_name("get")
                .value_name("key")
                .help("Get the value")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("rm")
                .value_name("key")
                .help("remove value")
                .takes_value(true)
        )
        .arg(&[
            Arg::with_name("set")
                .value_name("key")
                .takes_value(true),
            Arg::with_name("set")
                .value_name("key")
                .takes_value(true),
        ]
        )

        .get_matches();
}
