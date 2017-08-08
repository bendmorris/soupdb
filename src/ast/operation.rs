use ::ast::Identifier;
use ::model::Model;

pub struct InputDef {
    name: Identifier,
    type: Model,
}

pub struct Operation {
    inputs: Vec<InputDef>,
    outputType: ModelType,
}
