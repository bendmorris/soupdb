use ::ast::tuple::{TupleDef, TupleEntry};
use ::ast::value_type::ValueType;
use ::model::ModelType;

/// A TABLE is a collection of tuples ordered by an automatically-generated
/// rowid field.
///
/// ```sql
/// CREATE TABLE inventory (item_id int, name str, count int);
/// ```
///
/// Tuples in a TABLE have a hidden key, `rowid autoid`, which is automatically
/// incremented.
///
/// The TABLE model type supports SQL queries. These queries can interact with
/// other model types by utilizing operations which transform those models into
/// a value, tuple or collection of tuples and including them in the column
/// specification, JOIN or WHERE clauses. For example:
///
/// ```sql
/// SELECT col1, col2
/// FROM my_table
/// WHERE col > jsonpath(my_doc, `$.abc.def`);
/// ```
///
/// SQL queries return a vector of tuples (the base storage of the TABLE model)
/// as a result.
#[derive(Debug)]
pub struct Table {
    pub schema: TupleDef,
}

impl ModelType for Table {
    fn rowid_schema(&self) -> Option<TupleDef> {
        Some(TupleDef(vec![
            TupleEntry {name: "rowid".to_string(), value: ValueType::Uint}
        ]))
    }

    fn to_ddl(&self, name: &str) -> String {
        format!("create table {} {};", name, self.schema.to_ddl())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::ast::tuple::{TupleEntry, TupleDef};
    use ::ast::value_type::ValueType;
    use ::model::Model;

    #[test]
    fn test_table_ddl() {
        // test hand-written model AST against its DDL
        let test_ddl = "create table test_table (col_1 int, col_2 nullable vector(3) float);".to_string();
        assert_eq!(
            test_ddl,
            (Model {name: "test_table".to_string(), schema: Box::new(Table {schema: TupleDef(vec![
                TupleEntry {name: "col_1".to_string(), value: ValueType::Int},
                TupleEntry {name: "col_2".to_string(), value: ValueType::Nullable(Box::new(ValueType::Vector(3, Box::new(ValueType::Float))))},
            ])})}).to_ddl()
        );

        // parse the DDL into a create model command, check that the model can
        // then generate the same DDL
        let parsed_model = Model::from_ddl(&test_ddl).unwrap();
        assert_eq!(test_ddl, parsed_model.to_ddl());
    }
}
