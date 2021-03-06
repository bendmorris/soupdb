use ::ast::tuple::{TupleDef, TupleEntry};
use ::ast::value_type::ValueType;
use ::model::ModelType;

#[derive(Debug)]
pub struct GeoHash {
    pub schema: TupleDef,
}

impl ModelType for GeoHash {
    fn rowid_schema(&self) -> Option<TupleDef> {
        Some(TupleDef(vec![
            TupleEntry {name: "point".to_string(), value: ValueType::Vector(2, Box::new(ValueType::Float))}
        ]))
    }

    fn to_ddl(&self, name: &str) -> String {
        format!("create geohash {} {};", name, self.schema.to_ddl())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::ast::tuple::{TupleEntry, TupleDef};
    use ::ast::value_type::ValueType;
    use ::model::Model;

    #[test]
    fn test_geohash_ddl() {
        let test_ddl = "create geohash test_geohash (col_1 int, col_2 nullable vector(3) float);".to_string();

        assert_eq!(
            test_ddl,
            (Model {name: "test_geohash".to_string(), schema: Box::new(GeoHash {schema: TupleDef(vec![
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
