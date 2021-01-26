use std::collections::HashMap;

#[derive(Debug)]
pub struct KvStore {
    storage: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            storage: HashMap::new()
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Option<String> {
        self.storage.insert(key, value)
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        self.storage.get(&key).cloned()
    }

    pub fn remove(&mut self, key: String) -> Option<String> {
        self.storage.remove(&key)
    }
}