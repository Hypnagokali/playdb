pub mod access;

use crate::store::Store;

pub struct Database<S: Store> {
    pub name: String,
    store: S,
}

impl<S: Store> Database<S> {
    pub fn new(name: &str, store: S) -> Self {
        Self {
            name: name.to_string(),
            store,
        }
    }
    fn init(&self) {
        // create class, attribute and index tables
        // must generate PageDataLayout and store it somewhere
    }
}