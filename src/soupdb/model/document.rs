use soupdb::model::ModelType;
use soupdb::io::value::Value;

pub enum DocumentValue {
    ConcreteValue(Value),
    Array(Vec<Box<DocumentValue>>),
    SubDocument(Box<DocumentValue>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Document {}

impl ModelType for Document {
    fn to_ddl(&self, name: &str) -> String {
        format!("create document {};", name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use soupdb::model::Model;

    #[test]
    fn test_document_ddl() {
        let test_ddl = "create document test_doc;".to_string();

        assert_eq!(
            test_ddl,
            (Model {name: "test_doc".to_string(), schema: Box::new(Document {})}).to_ddl()
        );

        // parse the DDL into a create model command, check that the model can
        // then generate the same DDL
        let parsed_model = Model::from_ddl(&test_ddl).unwrap();
        assert_eq!(test_ddl, parsed_model.to_ddl());
    }
}
