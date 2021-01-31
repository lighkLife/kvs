use std::collections::HashMap;

/// The `KvStore` stores string key-value pairs.
///
/// Key-value pairs are stored in a `HashMap` in memory and it will be persisted to disk on the future version.
///
/// Example:
/// ```rust
/// # use kvs::KvStore;
/// let mut kvs = KvStore::new();
/// kvs.set("key".to_owned(), "value".to_owned());
/// assert_eq!(kvs.get("key".to_owned()), Some("value".to_owned()));
/// kvs.remove("key".to_owned());
/// assert_eq!(kvs.get("key".to_owned()), None);
/// ```
#[derive(Default)]
pub struct KvStore {
    storage: HashMap<String, String>,
}

impl KvStore {
    /// Create a KvStore instance.
    pub fn new() -> KvStore {
        KvStore {
            storage: HashMap::new()
        }
    }

    /// Set key to hold the string value.
    /// If key already holds a value, it is overwritten.
    pub fn set(&mut self, key: String, value: String) {
        self.storage.insert(key, value);
    }

    /// Get the value of key.
    /// If the key does not exist the special value None is returned.
    pub fn get(&mut self, key: String) -> Option<String> {
        self.storage.get(&key).cloned()
    }

    /// Remove the value of key.
    pub fn remove(&mut self, key: String) {
        self.storage.remove(&key);
    }
}

