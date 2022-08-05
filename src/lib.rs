#![deny(missing_docs)]
/*!
The KvStore store key/value pairs.
 */
mod client;
mod engine;
mod errors;
mod proto;
mod server;

pub mod thread_pool;

pub use client::Client;
pub use engine::Command;
pub use engine::{KvStore, KvsEngine, SledKvsEngine};
pub use errors::{KVStoreError, Result};
pub use proto::{Request, Response};
pub use server::{EngineType, KvServer};
