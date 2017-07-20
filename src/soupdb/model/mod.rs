pub mod document;
pub mod geohash;
pub mod graph;
pub mod table;
pub mod timeseries;

use std::fmt::Debug;
use std::io::Write;
use soupdb::error::{Error, Result};
use soupdb::tuple::TupleDef;

pub trait ModelType: Debug {
    fn rowid_schema(&self) -> Option<TupleDef> {
        None
    }
    fn to_ddl(&self, name: &str) -> String;
}

#[derive(Debug)]
pub struct Model {
    pub name: String,
    pub schema: Box<ModelType>,
}

impl Model {
    pub fn new(name: String, schema: Box<ModelType>) -> Self {
        Model {name: name, schema: schema}
    }

    pub fn from_ddl(ddl: &str) -> Result<Model> {
        use soupdb::command::Command;
        use soupdb::command::parse::parse_command;

        match parse_command(ddl) {
            Ok(Command::CreateModel {name: n, schema: s}) => Ok(Model {name: n, schema: s}),
            Ok(c) => Err(Error::ParseError(format!("invalid DDL: {:?}", c))),
            Err(e) => Err(e),
        }
    }

    pub fn to_ddl(&self) -> String {
        self.schema.to_ddl(&self.name)
    }

    pub fn write_schema(&self, mut to: &mut Write) {
        to.write(self.to_ddl().as_bytes()).unwrap();
    }
}
