use std::collections::HashMap;
use soupdb::io::dbfile::DbFile;

pub struct SoupDbServer {
    pub databases: HashMap<String, DbFile>,
    pub current_db: Option<String>,
}

impl SoupDbServer {
    pub fn new() -> SoupDbServer {
        SoupDbServer {
            databases: HashMap::new(),
            current_db: None,
        }
    }

    pub fn run(self) {

    }
}
