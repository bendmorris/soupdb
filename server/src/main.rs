pub mod soupdb;

extern crate argparse;
extern crate mio;

use argparse::{ArgumentParser, Store};
use soupdb::server::SoupDbServer;

fn main() {
    // parse CLI args
    let mut src = "localhost:27278".to_string();
    {
        let mut parser = ArgumentParser::new();
        parser.refer(&mut src).add_argument("address", Store, "host:port to listen on");

        parser.parse_args_or_exit();
    }

    let mut server = SoupDbServer::new();
    server.run();
}
