pub mod expr;
pub mod binop;
pub mod parse;

use soupdb::model::ModelType;

#[derive(Debug)]
pub enum Command {
    // database commands
    CreateDatabase {name: String, local_file: Option<String>},
    DropDatabase {name: String},
    UseDatabase {name: String},
    CleanDatabase {name: String},
    ImportDatabase {name: String, path: String},

    // model commands
    CreateModel {name: String, schema: Box<ModelType>},
    DropModel {name: String},
    Insert {name: String, cols: Option<Vec<String>>, values: Vec<String>},
}

impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self) == format!("{:?}", other)
    }
}
