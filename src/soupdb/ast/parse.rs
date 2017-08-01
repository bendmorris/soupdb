use nom::{IResult, digit};
use soupdb::{Error, Result};
use soupdb::ast::{Expr, BinaryOperator, UnaryOperator, Identifier};
use soupdb::ast::command::Command;
use soupdb::ast::binop::{ExprToken, shunting_yard};
use soupdb::ast::tuple::{TupleDef, TupleEntry};
use soupdb::ast::value_type::ValueType;
use soupdb::model::document::Document;
use soupdb::model::geohash::GeoHash;
use soupdb::model::graph::Graph;
use soupdb::model::table::Table;
use soupdb::model::timeseries::TimeSeries;

// basic subparsers

named!(identifier<&str, String>, do_parse!(
    first_char: is_a_s!("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_") >>
    chars: opt!(complete!(is_a_s!("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789"))) >>
    (format!("{}{}", first_char, match chars {
        Some(s) => s,
        _ => ""
    }))
));

named!(char_sequence<&str, &str>, do_parse!(
    chars: is_not!("\" \r\n") >>
    (chars)
));

named!(quoted_char_sequence<&str, &str>, do_parse!(
    char!('"') >>
    chars: opt!(is_not!("\"")) >>
    char!('"') >>
    (match chars {
        Some(x) => x,
        None => ""
    })
));

// values

named!(uint_literal_parser<&str, u64>, do_parse!(
    val: digit >>
    (val.parse::<u64>().unwrap())
));

named!(int_literal_parser<&str, i64>, do_parse!(
    sign: opt!(complete!(tag!("-"))) >>
    val: uint_literal_parser >>
    ((match sign {
        Some("-") => -1,
        _ => 1,
    }) * (val as i64))
));

named!(float_literal_parser<&str, f64>, do_parse!(
    base: digit >>
    dec: complete!(do_parse!(
        tag!(".") >>
        n: digit >>
        (n)
    )) >>
    (format!("{}.{}", base, dec).parse::<f64>().unwrap())
));

named!(string_literal_parser<&str, String>, do_parse!(
    chars: quoted_char_sequence >>
    (chars.to_string())
));

named!(true_literal_parser<&str, bool>, do_parse!(
    tag_no_case!("TRUE") >>
    (true)
));

named!(false_literal_parser<&str, bool>, do_parse!(
    tag_no_case!("FALSE") >>
    (false)
));

named!(bool_literal_parser<&str, bool>, alt_complete!(
    true_literal_parser |
    false_literal_parser
));

named!(null_literal_parser<&str, ()>, do_parse!(
    tag_no_case!("NULL") >>
    (())
));

// value schemas

named!(size_spec_parser<&str, u64>, do_parse!(
    char!('(') >>
    size: uint_literal_parser >>
    char!(')') >>
    (size)
));

named!(bool_valuetype_parser<&str, ValueType>, do_parse!(
    tag_no_case!("BOOL") >>
    (ValueType::Bool)
));

named!(uint_valuetype_parser<&str, ValueType>, ws!(do_parse!(
    tag_no_case!("UNSIGNED") >>
    tag_no_case!("INT") >>
    (ValueType::Uint)
)));

named!(int_valuetype_parser<&str, ValueType>, do_parse!(
    tag_no_case!("INT") >>
    (ValueType::Int)
));

named!(float_valuetype_parser<&str, ValueType>, do_parse!(
    tag_no_case!("FLOAT") >>
    (ValueType::Float)
));

named!(str_valuetype_parser<&str, ValueType>, do_parse!(
    tag_no_case!("STR") >>
    size: opt!(size_spec_parser) >>
    (ValueType::Str(match size {
        Some(s) => s,
        None => 0
    }))
));

named!(nullable_valuetype_parser<&str, ValueType>, ws!(do_parse!(
    tag_no_case!("NULLABLE") >>
    schema: valuetype_parser >>
    (ValueType::Nullable(Box::new(schema)))
)));

named!(vector_valuetype_parser<&str, ValueType>, ws!(do_parse!(
    tag_no_case!("VECTOR") >>
    char!('(') >>
    size: uint_literal_parser >>
    char!(')') >>
    schema: valuetype_parser >>
    (ValueType::Vector(size, Box::new(schema)))
)));

named!(valuetype_parser<&str, ValueType>, alt_complete!(
    bool_valuetype_parser |
    uint_valuetype_parser |
    int_valuetype_parser |
    float_valuetype_parser |
    str_valuetype_parser |
    nullable_valuetype_parser |
    vector_valuetype_parser
));

// tuples

named!(tuple_entry_parser<&str, TupleEntry>, ws!(do_parse!(
    name: identifier >>
    value: valuetype_parser >>
    (TupleEntry {name: name, value: value})
)));

named!(tuple_def_parser<&str, TupleDef>, ws!(do_parse!(
    char!('(') >>
    tuple_entries: separated_list_complete!(char!(','), tuple_entry_parser) >>
    char!(')') >>
    (TupleDef(tuple_entries))
)));

// expressions

named!(null_literal_expr_parser<&str, Expr>, ws!(do_parse!(
    value: null_literal_parser >>
    (Expr::Literal {value_type: ValueType::Unknown, value: "null".to_string()})
)));

named!(bool_literal_expr_parser<&str, Expr>, ws!(do_parse!(
    value: bool_literal_parser >>
    (Expr::Literal {value_type: ValueType::Bool, value: format!("{}", value)})
)));

named!(string_literal_expr_parser<&str, Expr>, ws!(do_parse!(
    value: string_literal_parser >>
    (Expr::Literal {value_type: ValueType::Str(0), value: value.to_string()})
)));

named!(int_literal_expr_parser<&str, Expr>, ws!(do_parse!(
    value: int_literal_parser >>
    (Expr::Literal {value_type: ValueType::Int, value: format!("{}", value)})
)));

named!(float_literal_expr_parser<&str, Expr>, ws!(do_parse!(
    value: float_literal_parser >>
    (Expr::Literal {value_type: ValueType::Float, value: format!("{}", value)})
)));

named!(literal_expr_parser<&str, Expr>, alt_complete!(
    null_literal_expr_parser |
    bool_literal_expr_parser |
    float_literal_expr_parser |
    int_literal_expr_parser |
    string_literal_expr_parser
));

named!(binop_parser<&str, BinaryOperator>, do_parse!(
    op: alt_complete!(
        tag_no_case!("and") |
        tag_no_case!("or") |
        tag!("*") |
        tag!("/") |
        tag!("+") |
        tag!("-") |
        tag!("=") |
        tag!("!=") |
        tag!("<=") |
        tag!(">=") |
        tag!("<") |
        tag!(">")
    ) >>
    (op.parse::<BinaryOperator>().unwrap())
));

named!(paren_expr_parser<&str, Expr>, ws!(do_parse!(
    char!('(') >>
    expr: expr_parser >>
    char!(')') >>
    (expr)
)));

named!(unop_expr_parser<&str, Expr>, ws!(do_parse!(
    op: alt_complete!(
        tag_no_case!("not")
    ) >>
    term: term_parser >>
    (Expr::UnOp {expr: Box::new(term), op: op.parse::<UnaryOperator>().unwrap()})
)));

named!(identifier_parser<&str, Expr>, do_parse!(
    id: identifier >>
    (Expr::Id(Identifier {name: id, qualifier: None}))
));

named!(qualified_identifier_parser<&str, Expr>, do_parse!(
    part1: identifier >>
    char!('.') >>
    part2: identifier >>
    (Expr::Id(Identifier {name: part2, qualifier: Some(part1)}))
));

named!(term_parser<&str, Expr>, alt_complete!(
    paren_expr_parser |
    unop_expr_parser |
    literal_expr_parser |
    qualified_identifier_parser |
    identifier_parser
));

named!(expr_parser<&str, Expr>, ws!(do_parse!(
    first: term_parser >>
    terms: fold_many0!(
        ws!(pair!(binop_parser, term_parser)),
        vec![ExprToken::Term(first)],
        |mut terms: Vec<ExprToken>, (op, val): (BinaryOperator, Expr)| {
            terms.push(ExprToken::BinOp(op));
            terms.push(ExprToken::Term(val));
            terms
        }
    ) >>
    (shunting_yard(terms))
)));

// commands

named!(create_table<&str, Command>, ws!(do_parse!(
    tag_no_case!("CREATE") >>
    tag_no_case!("TABLE") >>
    name: identifier >>
    tuple_def: tuple_def_parser >>
    char!(';') >>
    (Command::CreateModel {name: name, schema: Box::new(Table {schema: tuple_def})})
)));

named!(create_document<&str, Command>, ws!(do_parse!(
    tag_no_case!("CREATE") >>
    tag_no_case!("DOCUMENT") >>
    name: identifier >>
    char!(';') >>
    (Command::CreateModel {name: name, schema: Box::new(Document {})})
)));

named!(create_geohash<&str, Command>, ws!(do_parse!(
    tag_no_case!("CREATE") >>
    tag_no_case!("GEOHASH") >>
    name: identifier >>
    tuple_def: tuple_def_parser >>
    char!(';') >>
    (Command::CreateModel {name: name, schema: Box::new(GeoHash {schema: tuple_def})})
)));

named!(create_graph<&str, Command>, ws!(do_parse!(
    tag_no_case!("CREATE") >>
    tag_no_case!("GRAPH") >>
    name: identifier >>
    node_schema: tuple_def_parser >>
    edge_schema: tuple_def_parser >>
    char!(';') >>
    (Command::CreateModel {name: name, schema: Box::new(Graph {node_schema: node_schema, edge_schema: edge_schema})})
)));

named!(create_timeseries<&str, Command>, ws!(do_parse!(
    tag_no_case!("CREATE") >>
    tag_no_case!("TIMESERIES") >>
    name: identifier >>
    tuple_def: tuple_def_parser >>
    char!(';') >>
    (Command::CreateModel {name: name, schema: Box::new(TimeSeries {schema: tuple_def})})
)));

named!(create_command_parser<&str, Command>, alt_complete!(
    create_table |
    create_document |
    create_geohash |
    create_graph |
    create_timeseries
));

named!(command_parser<&str, Command>, alt_complete!(
    create_command_parser
));

/// Provides a nom parser wrapper which returns a soupdb::error::Result.
fn parser_wrapper<T, E: ::std::fmt::Debug>(parser: &Fn(&str) -> IResult<&str, T, E>, input: &str) -> Result<T> {
    match parser(input) {
        IResult::Done("", v) => Ok(v),
        IResult::Done(s, v) => Err(Error::ParseError(format!("Parsed statement contained additional unparsed content: {:?}", s))),
        IResult::Error(e) => Err(Error::ParseError(format!("Parse error: {:?}", e))),
        IResult::Incomplete(e) => Err(Error::ParseError(format!("Could not parse a complete statement: \"{:?}\"", e))),
    }
}

pub fn parse_command(input: &str) -> Result<Command> {
    parser_wrapper(&command_parser, input)
}

pub fn parse_ddl(input: &str) -> Result<Command> {
    parser_wrapper(&command_parser, input)
}

pub fn parse_expr(input: &str) -> Result<Expr> {
    parser_wrapper(&expr_parser, input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create() {
        assert_eq!(
            parse_command("create TABLE my_table (col_1 int, col2 str, col3 nullable bool, d nullable str(10), column_5 vector(3) unsigned int);"),
            Ok(Command::CreateModel {name: "my_table".to_string(), schema: Box::new(Table {schema: TupleDef(vec![
                TupleEntry {name: "col_1".to_string(), value: ValueType::Int},
                TupleEntry {name: "col2".to_string(), value: ValueType::Str(0)},
                TupleEntry {name: "col3".to_string(), value: ValueType::Nullable(Box::new(ValueType::Bool))},
                TupleEntry {name: "d".to_string(), value: ValueType::Nullable(Box::new(ValueType::Str(10)))},
                TupleEntry {name: "column_5".to_string(), value: ValueType::Vector(3, Box::new(ValueType::Uint))},
            ])})})
        );

        assert_eq!(
            parse_command("CREATE DOCUMENT doc ;"),
            Ok(Command::CreateModel {name: "doc".to_string(), schema: Box::new(Document {})})
        );
    }

    #[test]
    fn test_parse_expr() {
        assert_eq!(
            parse_expr("FALSE"),
            Ok(Expr::Literal {value_type: ValueType::Bool, value: "false".to_string()})
        );

        assert_eq!(
            parse_expr("1 + 2"),
            Ok(Expr::BinOp {
                left: Box::new(Expr::Literal {value_type: ValueType::Int, value: "1".to_string()}),
                op: BinaryOperator::OpAdd,
                right: Box::new(Expr::Literal {value_type: ValueType::Int, value: "2".to_string()}),
            })
        );

        assert_eq!(
            parse_expr("1 + 2 * 3"),
            Ok(Expr::BinOp {
                left: Box::new(Expr::Literal {value_type: ValueType::Int, value: "1".to_string()}),
                op: BinaryOperator::OpAdd,
                right: Box::new(
                    Expr::BinOp {
                        left: Box::new(Expr::Literal {value_type: ValueType::Int, value: "2".to_string()}),
                        op: BinaryOperator::OpMul,
                        right: Box::new(Expr::Literal {value_type: ValueType::Int, value: "3".to_string()}),
                    }
                ),
            })
        );

        assert_eq!(
            parse_expr("1 + 2 * 3 - (4 + 5) / 6-7.1"),
            Ok(Expr::BinOp {
                left: Box::new(Expr::BinOp {
                    left: Box::new(Expr::BinOp {
                        left: Box::new(Expr::Literal {value_type: ValueType::Int, value: "1".to_string()}),
                        op: BinaryOperator::OpAdd,
                        right: Box::new(
                            Expr::BinOp {
                                left: Box::new(Expr::Literal {value_type: ValueType::Int, value: "2".to_string()}),
                                op: BinaryOperator::OpMul,
                                right: Box::new(Expr::Literal {value_type: ValueType::Int, value: "3".to_string()}),
                            }
                        ),
                    }),
                    op: BinaryOperator::OpSub,
                    right: Box::new(Expr::BinOp {
                        left: Box::new(Expr::BinOp {
                            left: Box::new(Expr::Literal {value_type: ValueType::Int, value: "4".to_string()}),
                            op: BinaryOperator::OpAdd,
                            right: Box::new(Expr::Literal {value_type: ValueType::Int, value: "5".to_string()}),
                        }),
                        op: BinaryOperator::OpDiv,
                        right: Box::new(Expr::Literal {value_type: ValueType::Int, value: "6".to_string()}),
                    }),
                }),
                op: BinaryOperator::OpSub,
                right: Box::new(Expr::Literal {value_type: ValueType::Float, value: "7.1".to_string()}),
            })
        );

        assert_eq!(
            parser_wrapper(&identifier, "def"),
            Ok("def".to_string())
        );

        assert_eq!(
            parser_wrapper(&term_parser, "def"),
            Ok(Expr::Id(Identifier {name: "def".to_string(), qualifier: None}))
        );

        assert_eq!(
            parse_expr("def"),
            Ok(Expr::Id(Identifier {name: "def".to_string(), qualifier: None}))
        );

        assert_eq!(
            parse_expr("abc.def"),
            Ok(Expr::Id(Identifier {name: "def".to_string(), qualifier: Some("abc".to_string())}))
        );

        assert_eq!(
            parse_expr("1 + def"),
            Ok(Expr::BinOp {
                left: Box::new(Expr::Literal {value_type: ValueType::Int, value: "1".to_string()}),
                op: BinaryOperator::OpAdd,
                right: Box::new(Expr::Id(Identifier {name: "def".to_string(), qualifier: None}))
            })
        );

        assert_eq!(
            parse_expr("1 + abc.def"),
            Ok(Expr::BinOp {
                left: Box::new(Expr::Literal {value_type: ValueType::Int, value: "1".to_string()}),
                op: BinaryOperator::OpAdd,
                right: Box::new(Expr::Id(Identifier {name: "def".to_string(), qualifier: Some("abc".to_string())}))
            })
        );
    }
}
