use std::process::exit;

use structopt::StructOpt;

fn main() {
    let cmd = Cmd::from_args() as Cmd;
    match cmd {
        Cmd::Set { key: _, value: _ } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Cmd::Get { key: _ } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Cmd::Rm { key: _ } => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
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
