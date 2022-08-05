use crate::{KVStoreError, Request, Response, Result};
use serde::Deserialize;
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;

/// a tcp client which can connect to kvs-server
pub struct Client {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl Client {
    /// init a client
    pub fn new(addr: &str) -> Result<Client> {
        let stream = TcpStream::connect(addr)?;
        Ok(Client {
            reader: Deserializer::from_reader(BufReader::new(stream.try_clone()?)),
            writer: BufWriter::new(stream),
        })
    }

    /// perform a request
    pub fn request(&mut self, request: &Request) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, request)?;
        self.writer.flush()?;
        match Response::deserialize(&mut self.reader)? {
            Response::Ok(value) => Ok(value),
            Response::Err(err) => Err(KVStoreError::CommonStringError(err)),
        }
    }
}
