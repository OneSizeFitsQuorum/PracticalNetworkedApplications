use crate::Result;
use serde::{Deserialize, Serialize};

mod kv;
mod sled;

pub use self::kv::KvStore;
pub use self::sled::SledKvsEngine;

/// A trait which supports pluggable storage engines
pub trait KvsEngine: Clone + Send + 'static {
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// Get the string value of a string key. If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Remove a given string key.
    /// Return an error if the key does not exit or value is not read successfully.
    fn remove(&self, key: String) -> Result<()>;
}

/// a struct which supports serialization and deserialization
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    /// for set command
    SET(String, String),
    /// for rm command
    RM(String),
}
