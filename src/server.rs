use crate::Result;
use crate::{KvsEngine, Request, Response};
use log::info;
use serde::Deserialize;
use serde_json::Deserializer;
use std::fmt;
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};

/// a generic KvServer which supports pluggable storage engines
pub struct KvServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvServer<E> {
    /// create server with engine
    pub fn new(engine: E) -> Self {
        KvServer { engine }
    }

    /// serve at addr to handle requests
    pub fn serve(&mut self, addr: &String) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let stream = stream?;

            self.handle_connection(stream)?;
        }
        Ok(())
    }

    fn handle_connection(&mut self, mut stream: TcpStream) -> Result<()> {
        let request =
            Request::deserialize(&mut Deserializer::from_reader(BufReader::new(&mut stream)))?;

        info!("Request: {:?}", &request);

        let response;
        match request {
            Request::SET(key, value) => {
                match self.engine.set(key, value) {
                    Ok(_) => response = Response::Ok(None),
                    Err(err) => response = Response::Err(format!("{}", err)),
                };
            }
            Request::RM(key) => {
                match self.engine.remove(key) {
                    Ok(_) => response = Response::Ok(None),
                    Err(err) => response = Response::Err(format!("{}", err)),
                };
            }
            Request::GET(key) => {
                match self.engine.get(key) {
                    Ok(value) => response = Response::Ok(value),
                    Err(err) => response = Response::Err(format!("{}", err)),
                };
            }
        }

        info!("Response: {:?}", &response);

        serde_json::to_writer(stream, &response)?;

        Ok(())
    }
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
