## Overvire
The cargo project, `kvs`, builds a command-line key-value store client called
`kvs-client`, and a key-value store server called `kvs-server`, both of which in
turn call into a library called `kvs`. The client speaks to the server over
a custom protocol.

**kvs-server**
```bash
lighk$kvs-server -h
kvs-server 0.1.0
A key-value storage server.

USAGE:
    kvs-server [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --addr <IP:PORT>          Set ip address and port number with the format IP:PORT. [default: 127.0.0.1:4000]
        --engine <ENGINE-NAME>    Set storage engines, either kvs or sled. [possible values: kvs, sled]
```

```bash
lighk$kvs-client -h
kvs-client 0.1.0
A client for kvs server.

USAGE:
    kvs-client <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    get     Get the string value of a given string key.
    help    Prints this message or the help of the given subcommand(s)
    rm      Remove a given key.
    set     Set the value of a string key to a string.
```


## Project spec

- `kvs-server [--addr IP-PORT] [--engine ENGINE-NAME]`

  Start the server and begin listening for incoming connections. `--addr`
  accepts an IP address, either v4 or v6, and a port number, with the format
  `IP:PORT`. If `--addr` is not specified then listen on `127.0.0.1:4000`.

  If `--engine` is specified, then `ENGINE-NAME` must be either "kvs", in which
  case the built-in engine is used, or "sled", in which case sled is used. If
  this is the first run (there is no data previously persisted) then the default
  value is "kvs"; if there is previously persisted data then the default is the
  engine already in use. If data was previously persisted with a different
  engine than selected, print an error and exit with a non-zero exit code.

  Print an error and return a non-zero exit code on failure to bind a socket, if
  `ENGINE-NAME` is invalid, if `IP-PORT` does not parse as an address.

- `kvs-server -V`

  Print the version.

The `kvs-client` executable supports the following command line arguments:

- `kvs-client set <KEY> <VALUE> [--addr IP-PORT]`

  Set the value of a string key to a string.

  `--addr` accepts an IP address, either v4 or v6, and a port number, with the
  format `IP:PORT`. If `--addr` is not specified then connect on
  `127.0.0.1:4000`.

  Print an error and return a non-zero exit code on server error,
  or if `IP-PORT` does not parse as an address.

- `kvs-client get <KEY> [--addr IP-PORT]`

  Get the string value of a given string key.

  `--addr` accepts an IP address, either v4 or v6, and a port number, with the
  format `IP:PORT`. If `--addr` is not specified then connect on
  `127.0.0.1:4000`.

  Print an error and return a non-zero exit code on server error,
  or if `IP-PORT` does not parse as an address.

- `kvs-client rm <KEY> [--addr IP-PORT]`

  Remove a given string key.

  `--addr` accepts an IP address, either v4 or v6, and a port number, with the
  format `IP:PORT`. If `--addr` is not specified then connect on
  `127.0.0.1:4000`.

  Print an error and return a non-zero exit code on server error,
  or if `IP-PORT` does not parse as an address. A "key not found" is also
  treated as an error in the "rm" command.

- `kvs-client -V`

  Print the version.
