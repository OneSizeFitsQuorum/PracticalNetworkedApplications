/*!
The KvStore store key/value pairs.
 */
#![deny(missing_docs)]

use std::collections::HashMap;

/** A KvStore stores key/value pairs in HashMap.
# Example
```
use kvs::KvStore;
let mut store = KvStore::new();

store.set("1".to_owned(),"1".to_owned());
assert_eq!(store.get("1".to_owned()),Some("1".to_owned()));

store.remove("1".to_owned());
assert_eq!(store.get("1".to_owned()), None);
```
*/
pub struct KvStore {
    data: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    /// generate a new KvStore which can serve reads and writes
    pub fn new() -> KvStore {
        KvStore {
            data: HashMap::new(),
        }
    }

    /// Set the value of a string key to a string
    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    /// Get the string value of a given string key
    pub fn get(&self, key: String) -> Option<String> {
        self.data.get(&key).cloned()
    }

    /// Remove a given key
    pub fn remove(&mut self, key: String) {
        self.data.remove(&key);
    }
}
