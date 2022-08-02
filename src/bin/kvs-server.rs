use clap::{arg, command, ArgMatches};
use kvs::{EngineType, KVStoreError, KvServer, KvStore, KvsEngine, Result, SledKvsEngine};
use log::{info, LevelFilter};
use std::{env, process};

fn main() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let matches = command!()
        .name("kvs-server")
        .arg(
            arg!(--addr <IPPORT>)
                .required(false)
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            arg!(--engine <ENGINENAME>)
                .required(false)
                .value_parser(["kvs", "sled"]),
        )
        .get_matches();
    if let Err(err) = init(matches) {
        eprintln!("{:?}", err);
        process::exit(-1);
    }
    Ok(())
}

fn init(matches: ArgMatches) -> Result<()> {
    let addr = matches.get_one::<String>("addr").unwrap();
    let engine_type = judge_engine(matches.get_one::<String>("engine").cloned())?;

    info!("Version: [{}]", env!("CARGO_PKG_VERSION"));
    info!("Addr: [{}]", addr);
    info!("Engine: [{}]", engine_type);

    match engine_type {
        EngineType::KvStore => run_server(
            KvStore::open(env::current_dir()?.join(EngineType::KvStore.to_string()))?,
            addr,
        ),
        EngineType::SledKvsEngine => run_server(
            SledKvsEngine::open(env::current_dir()?.join(EngineType::SledKvsEngine.to_string()))?,
            addr,
        ),
    }
}

fn judge_engine(engine: Option<String>) -> Result<EngineType> {
    let dir = env::current_dir()?;
    match engine {
        None => {
            if dir.join(EngineType::SledKvsEngine.to_string()).exists() {
                return Ok(EngineType::SledKvsEngine);
            }
            Ok(EngineType::KvStore)
        }
        Some(v) => {
            if v == EngineType::KvStore.to_string() {
                if dir.join(EngineType::SledKvsEngine.to_string()).exists() {
                    return Err(KVStoreError::ChangeEngineError);
                }
                Ok(EngineType::KvStore)
            } else {
                if dir.join(EngineType::KvStore.to_string()).exists() {
                    return Err(KVStoreError::ChangeEngineError);
                }
                Ok(EngineType::SledKvsEngine)
            }
        }
    }
}

fn run_server<E: KvsEngine>(engine: E, addr: &String) -> Result<()> {
    let mut server = KvServer::new(engine);
    server.serve(addr)?;
    Ok(())
}
