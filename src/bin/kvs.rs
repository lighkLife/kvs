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
    #[structopt(about = "Set key to hold the string value.")]
    Set {
        key: String,
        value: String,
    },

    #[structopt(about = "Get the value of key.")]
    Get {
        key: String
    },

    #[structopt(about = "Remove the value of key.")]
    Rm {
        key: String
    },
}
