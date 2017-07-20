use soupdb::model::Model;

pub struct InputDef {
    name:String,
    type:Model,
}

pub struct Operation {
    inputs:Vec<InputDef>,
    outputType:ModelType,
}
