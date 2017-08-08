use ::model::ModelType;
use ::io::value::Value;

pub enum DocumentValue {
    ConcreteValue(Value),
    Array(Vec<Box<DocumentValue>>),
    SubDocument(Box<DocumentValue>),
}

/// A DOCUMENT contains a (potentially nested) mapping of keys to values, other
/// documents, or vectors of documents. Documents are schemaless and their structure
/// can be altered by adding or removing values.
///
/// ```sql
/// CREATE DOCUMENT my_document;
/// ```
///
/// The JSONPath query language can be used to drill down into a DOCUMENT model and
/// extract values. These queries return a DOCUMENT, which may contain only a single
/// key/value pair or a subset of the queried DOCUMENT. JSONPath queries may use
/// object or bracket notation for attribute access.
///
/// For the following document:
///
/// ```js
/// {
///     "name": "Bob",
///     "age": 35,
///     "children": [
///         {"name": "Margaret", age: 7},
///         {"name": "David", age: 3},
///     ]
/// }
/// ```
///
/// The following are examples of valid queries:
///
/// ```js
/// $.name              // {"name": "Bob"}
/// $['age']            // {"age": 35}
/// $.children          // {"children": [...]}
/// $.children[0].name  // {"name": "Margaret"}
/// $.children[1]       // {"name": "David", "age": 3}
/// ```
///
/// SoupDB's parser will parse any text between accents (\`\`) as an unparsed text
/// expression node, which will be passed in tact to any operations using it for
/// them to interpret. This allows strongly-typed, model-specific handling of custom
/// data formats such as query languages. These values cannot be coerced to any
/// other type. The contents of these unparsed sections can use `${expression}` to
/// interpolate expressions from the parent context.
///
/// Documents can be automatically coerced into tuples (discarding any non-value
/// key/value pairs) and documents of a single key/value can be coerced into single
/// values.
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
    use ::model::Model;

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
