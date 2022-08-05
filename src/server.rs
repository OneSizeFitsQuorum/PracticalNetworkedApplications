use crate::thread_pool::ThreadPool;
use crate::Result;
use crate::{KvsEngine, Request, Response};
use log::{debug, error};
use serde::Deserialize;
use serde_json::Deserializer;
use std::fmt;
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

/// a generic KvServer which supports pluggable storage engines
pub struct KvServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
    is_stop: Arc<AtomicBool>,
}

impl<E: KvsEngine, P: ThreadPool> KvServer<E, P> {
    /// create server with engine
    pub fn new(engine: E, pool: P, is_stop: Arc<AtomicBool>) -> Self {
        KvServer {
            engine,
            pool,
            is_stop,
        }
    }

    /// serve at addr to handle requests
    pub fn serve(&mut self, addr: &String) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            if self.is_stop.load(Ordering::SeqCst) {
                break;
            }
            let engine = self.engine.clone();
            self.pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(err) = handle_connection(engine, stream) {
                        error!("Unexpected error occurs when serving request: {:?}", err)
                    }
                }
                Err(err) => error!(
                    "Unexpected error occurs when serving incoming request {:?}",
                    err
                ),
            })
        }
        Ok(())
    }
}

fn handle_connection<E: KvsEngine>(engine: E, mut stream: TcpStream) -> Result<()> {
    let request =
        Request::deserialize(&mut Deserializer::from_reader(BufReader::new(&mut stream)))?;

    let now = SystemTime::now();
    debug!("Request: {:?}", &request);

    let response;
    match request {
        Request::SET(key, value) => {
            match engine.set(key, value) {
                Ok(_) => response = Response::Ok(None),
                Err(err) => response = Response::Err(format!("{}", err)),
            };
        }
        Request::RM(key) => {
            match engine.remove(key) {
                Ok(_) => response = Response::Ok(None),
                Err(err) => response = Response::Err(format!("{}", err)),
            };
        }
        Request::GET(key) => {
            match engine.get(key) {
                Ok(value) => response = Response::Ok(value),
                Err(err) => response = Response::Err(format!("{}", err)),
            };
        }
    }

    debug!("Response: {:?}, {:?}", &response, now.elapsed());

    serde_json::to_writer(stream, &response)?;

    Ok(())
}

/// Indicates the type of engine
#[derive(Debug)]
pub enum EngineType {
    /// for KvStore
    KvStore,
    /// for SledKvsEngine
    SledKvsEngine,
}

impl fmt::Display for EngineType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EngineType::KvStore => write!(f, "kvs"),
            EngineType::SledKvsEngine => write!(f, "sled"),
        }
    }
}
