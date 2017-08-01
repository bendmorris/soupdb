use soupdb::ast::tuple::{TupleDef, TupleEntry};
use soupdb::ast::value_type::ValueType;
use soupdb::model::ModelType;

#[derive(Debug)]
pub struct TimeSeries {
    pub schema: TupleDef,
}

impl ModelType for TimeSeries {
    fn rowid_schema(&self) -> Option<TupleDef> {
        Some(TupleDef(vec![
            TupleEntry {name: "timestamp".to_string(), value: ValueType::Float}
        ]))
    }

    fn to_ddl(&self, name: &str) -> String {
        format!("create timeseries {} {};", name, self.schema.to_ddl())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use soupdb::ast::tuple::{TupleEntry, TupleDef};
    use soupdb::ast::value_type::ValueType;
    use soupdb::model::Model;

    #[test]
    fn test_timeseries_ddl() {


        let test_ddl = "create timeseries test_ts (col_1 int, col_2 nullable vector(3) float);".to_string();

        assert_eq!(
            test_ddl,
            (Model {name: "test_ts".to_string(), schema: Box::new(TimeSeries {schema: TupleDef(vec![
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
