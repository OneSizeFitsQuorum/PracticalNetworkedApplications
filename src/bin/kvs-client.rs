use clap::{arg, command, ArgMatches, SubCommand};
use kvs::{Client, Request, Result};
use std::string::String;
use std::{env, process};

fn main() {
    let matches = command!()
        .name("kvs-Client")
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string. Return an error if the value is not written successfully.")
                .arg(arg!(<KEY>))
                .arg(arg!(<VALUE>))
                .arg(arg!(--addr <IPPORT>).required(false).default_value("127.0.0.1:4000")),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.")
                .arg(arg!(<KEY>))
                .arg(arg!(--addr <IPPORT>).required(false).default_value("127.0.0.1:4000")),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key. Return an error if the key does not exist or is not removed successfully.c")
                .arg(arg!(<KEY>))
                .arg(arg!(--addr <IPPORT>).required(false).default_value("127.0.0.1:4000")),
        )
        .get_matches();
    if let Err(err) = send_request(matches) {
        eprintln!("{:?}", err);
        process::exit(-1);
    }
}

fn send_request(matches: ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let addr = sub_matches.get_one::<String>("addr").unwrap();
            let key = sub_matches.get_one::<String>("KEY").unwrap();
            let value = sub_matches.get_one::<String>("VALUE").unwrap();
            let mut client = Client::new(addr)?;
            client.request(&Request::SET(key.to_owned(), value.to_owned()))?;
        }
        Some(("get", sub_matches)) => {
            let addr = sub_matches.get_one::<String>("addr").unwrap();
            let key = sub_matches.get_one::<String>("KEY").unwrap();
            let mut client = Client::new(addr)?;
            match client.request(&Request::GET(key.to_owned()))? {
                None => println!("Key not found"),
                Some(value) => println!("{}", value),
            };
        }
        Some(("rm", sub_matches)) => {
            let addr = sub_matches.get_one::<String>("addr").unwrap();
            let key = sub_matches.get_one::<String>("KEY").unwrap();
            let mut client = Client::new(addr)?;
            client.request(&Request::RM(key.to_owned()))?;
        }
        _ => process::exit(-1),
    }
    Ok(())
}
