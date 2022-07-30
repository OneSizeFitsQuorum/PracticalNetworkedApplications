use clap::{arg, command, SubCommand};
use kvs::KvStore;
use kvs::{KVStoreError, Result};
use std::{env, process};

fn main() -> Result<()> {
    let matches = command!()
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string. Return an error if the value is not written successfully.")
                .arg(arg!(<KEY>))
                .arg(arg!(<VALUE>)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.")
                .arg(arg!(<KEY>)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key. Return an error if the key does not exist or is not removed successfully.c")
                .arg(arg!(<KEY>)),
        )
        .get_matches();

    let mut store = KvStore::open(env::current_dir()?)?;
    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").unwrap();
            let value = sub_matches.get_one::<String>("VALUE").unwrap();
            if let Err(err) = store.set(key.to_owned(), value.to_owned()) {
                println!("{:?}", err);
                process::exit(-1);
            };
        }
        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").unwrap();
            match store.get(key.to_owned())? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
        }
        Some(("rm", sub_matches)) => {
            let key = sub_matches.get_one::<String>("KEY").unwrap();
            if let Err(KVStoreError::KeyNotFound) = store.remove(key.to_owned()) {
                println!("Key not found");
                process::exit(-1)
            }
        }
        _ => process::exit(-1),
    }
    Ok(())
}
