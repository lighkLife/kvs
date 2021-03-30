use std::process::exit;

use structopt::StructOpt;
use kvs::{KvStore, KvsError, Result};
use std::env::current_dir;

fn main() -> Result<()> {
    let cmd = Cmd::from_args() as Cmd;

    match cmd {
        Cmd::Set { key, value } => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key, value)?;
        }
        Cmd::Get { key } => {
            let mut store = KvStore::open(current_dir()?)?;
            store.get("key".to_owned());
            if let Some(value) = store.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Cmd::Rm { key } => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}


#[derive(Debug, StructOpt)]
#[structopt()]
enum Cmd {
    #[structopt(about = "Set the value of a string key to a string. Print an error and return a non-zero exit code on failure.")]
    Set {
        key: String,
        value: String,
    },

    #[structopt(about = "Get the string value of a given string key. Print an error and return a non-zero exit code on failure.")]
    Get {
        key: String
    },

    #[structopt(about = "Remove a given key. Print an error and return a non-zero exit code on failure.")]
    Rm {
        key: String
    },
}
