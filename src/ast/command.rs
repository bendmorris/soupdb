use ::ast::{Expr, Identifier};
use ::model::ModelType;

pub type OrderByClause = Option<Vec<Expr>>;
pub type LimitClause = Option<u64>;

#[derive(Debug, PartialEq)]
pub enum SelectColumns {
    All,
    Named(Vec<(Expr, Option<String>)>),
}

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
    Select {
        cols: SelectColumns,
        from: Option<Vec<(String, Option<String>)>>,
        where_expr: Option<Expr>,
        group_by: Option<Vec<Expr>>,
        having: Option<Expr>,
        order_by: OrderByClause,
        limit: LimitClause,
    },
    Update {
        model: Identifier,
        where_expr: Option<Expr>,
        set: Vec<(Identifier, Expr)>,
        order_by: OrderByClause,
        limit: LimitClause,
    },
    Insert {
        model: Identifier,
        cols: Option<Vec<Identifier>>,
        values: Vec<Vec<Expr>>,
    },
    Delete {
        model: Identifier,
        where_expr: Option<Expr>,
        order_by: OrderByClause,
        limit: LimitClause,
    }
}

impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        format!("{:?}", self) == format!("{:?}", other)
    }
}
