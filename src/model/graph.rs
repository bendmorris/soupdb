use ::ast::tuple::TupleDef;
use ::model::ModelType;

/// A GRAPH is a directed graph structure, with a separate schema for nodes and
/// edges, both of which are tuples.
///
/// ```sql
/// CREATE GRAPH
/// WITH NODE (node_id int, node_label str)
/// WITH EDGE (edge_length int);
/// ```
#[derive(Debug)]
pub struct Graph {
    pub node_schema: TupleDef,
    pub edge_schema: TupleDef,
}

impl ModelType for Graph {
    fn to_ddl(&self, name: &str) -> String {
        format!("create graph {} {} {};", name, self.node_schema.to_ddl(), self.edge_schema.to_ddl())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::ast::tuple::{TupleEntry, TupleDef};
    use ::ast::value_type::ValueType;
    use ::model::Model;

    #[test]
    fn test_graph_ddl() {
        let test_ddl = "create graph test_graph (col_1 int, col_2 nullable vector(3) float) (edge_length float);".to_string();

        assert_eq!(
            test_ddl,
            (Model {name: "test_graph".to_string(), schema: Box::new(Graph {node_schema: TupleDef(vec![
                TupleEntry {name: "col_1".to_string(), value: ValueType::Int},
                TupleEntry {name: "col_2".to_string(), value: ValueType::Nullable(Box::new(ValueType::Vector(3, Box::new(ValueType::Float))))},
            ]), edge_schema: TupleDef(vec![
                TupleEntry {name: "edge_length".to_string(), value: ValueType::Float},
            ])})}).to_ddl()
        );

        // parse the DDL into a create model command, check that the model can
        // then generate the same DDL
        let parsed_model = Model::from_ddl(&test_ddl).unwrap();
        assert_eq!(test_ddl, parsed_model.to_ddl());
    }
}
