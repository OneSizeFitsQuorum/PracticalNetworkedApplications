use clap::{arg, command, SubCommand};
use std::process;

fn main() {
    let matches = command!()
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(arg!(<KEY>))
                .arg(arg!(<VALUE>)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(arg!(<KEY>)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(arg!(<KEY>)),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            println!(
                "KEY{:?}, VALUE{:?}",
                sub_matches.get_one::<String>("KEY").unwrap(),
                sub_matches.get_one::<String>("VALUE").unwrap()
            );
            eprintln!("unimplemented");
            process::exit(-1)
        }
        Some(("get", sub_matches)) => {
            println!("KEY{:?}", sub_matches.get_one::<String>("KEY").unwrap());
            eprintln!("unimplemented");
            process::exit(-1)
        }
        Some(("rm", sub_matches)) => {
            println!("KEY{:?}", sub_matches.get_one::<String>("KEY").unwrap());
            eprintln!("unimplemented");
            process::exit(-1)
        }
        _ => process::exit(-1),
    }
}
