#![deny(missing_docs)]
/*!
The KvStore store key/value pairs.
 */
mod command;
mod errors;
mod kv;

pub use command::Command;
pub use errors::{KVStoreError, Result};
pub use kv::KvStore;
