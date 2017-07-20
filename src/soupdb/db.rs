use std::collections::HashMap;
use std::path::PathBuf;
use glob::glob;
use soupdb::command::Command;
use soupdb::config::Config;
use soupdb::error::{Error, Result};
use soupdb::model::Model;

struct Database {
    pub name: String,
    pub config: Config,
    pub data_dir: PathBuf,
    pub schemas: HashMap<String, Model>,
}

impl Database {
    pub fn new(name: String, config: Option<Config>) -> Database {
        let config = match config {
            Some(c) => c,
            None => Config::new(),
        };

        let data_dir = config.data_dir.join(&name);

        let mut schemas = HashMap::new();
        for entry in glob(data_dir.join("*.schema").to_str().unwrap()) {
            // TODO: read in schemas
        }

        Database {
            name: name,
            config: config,
            data_dir: data_dir,
            schemas: schemas,
        }
    }

    pub fn run_command(command: Command) -> Result<()> {
        match command {
            _ => {
                Err(Error::NotYetImplemented)
            }
        }
    }
}

#[test]
fn test_database() {
    // make sure constructor works
    let db = Database::new("test_db".to_string(), None);
    assert_eq!(db.config.data_dir, Config::new().data_dir)
}
