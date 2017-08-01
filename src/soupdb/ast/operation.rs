use soupdb::ast::Identifier;
use soupdb::model::Model;

pub struct InputDef {
    name: Identifier,
    type: Model,
}

pub struct Operation {
    inputs: Vec<InputDef>,
    outputType: ModelType,
}
