use crate::{KVStoreError, KvsEngine, Result};
use sled::Db;
use std::path::PathBuf;

/** A KvStore stores key/value pairs using sled.
# Example
```
use std::env;
use kvs::{SledKvsEngine, Result};
use crate::kvs::KvsEngine;
# fn try_main() -> Result<()> {

let mut store = SledKvsEngine::open(env::current_dir()?)?;

store.set("1".to_owned(),"1".to_owned())?;
assert_eq!(store.get("1".to_owned())?, Some("1".to_owned()));

store.remove("1".to_owned())?;
assert_eq!(store.get("1".to_owned())?, None);
# Ok(())
# }
*/
pub struct SledKvsEngine {
    inner: Db,
}

impl SledKvsEngine {
    /// Open the SledKvsEngine at a given path. Return the SledKvsEngine.
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        Ok(SledKvsEngine {
            inner: sled::open(path.into())?,
        })
    }
}

impl KvsEngine for SledKvsEngine {
    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.inner.insert(key, value.into_bytes())?;
        // self.inner.flush()?;
        Ok(())
    }

    /// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self
            .inner
            .get(key)?
            .map(|ivec| ivec.to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    /// Remove a given key. Return an error if the key does not exist or is not removed successfully.
    fn remove(&mut self, key: String) -> Result<()> {
        self.inner.remove(key)?.ok_or(KVStoreError::KeyNotFound)?;
        self.inner.flush()?;
        Ok(())
    }
}
