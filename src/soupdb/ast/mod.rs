pub mod binop;
pub mod command;
pub mod parse;
pub mod tuple;
pub mod value_type;

use std::result::Result;
use std::str::FromStr;
use soupdb::Error;
use soupdb::ast::value_type::ValueType;

#[derive(Debug, PartialEq, Clone)]
pub struct Identifier {
    pub name: String,
    pub qualifier: Option<String>
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expr {
    Id(Identifier),
    Literal {value_type: ValueType, value: String},
    FunctionCall {name: String, args: Vec<Expr>},
    UnOp {expr: Box<Expr>, op: UnaryOperator},
    BinOp {left: Box<Expr>, right: Box<Expr>, op: BinaryOperator},
}

#[derive(PartialEq, Debug, Clone)]
pub enum BinaryOperator {
    OpMul,
    OpDiv,
    OpAdd,
    OpSub,
    OpEq,
    OpNeq,
    OpLt,
    OpGt,
    OpLte,
    OpGte,
    OpIs,
    OpLike,
    OpIn,
    OpAnd,
    OpOr,
}

impl BinaryOperator {
    pub fn precedence(&self) -> u8 {
        use self::BinaryOperator::*;
        match *self {
            OpMul | OpDiv => 5,
            OpAdd | OpSub => 4,
            OpEq | OpNeq | OpLt | OpGt | OpLte | OpGte | OpIs | OpLike | OpIn => 3,
            OpAnd | OpOr => 2,
        }
    }
}

impl FromStr for BinaryOperator {
    type Err = Error;
    fn from_str(s: &str) -> Result<BinaryOperator, Error> {
        use self::BinaryOperator::*;
        match s {
            "*" => Ok(OpMul),
            "/" => Ok(OpDiv),
            "+" => Ok(OpAdd),
            "-" => Ok(OpSub),
            "=" => Ok(OpEq),
            "!=" => Ok(OpNeq),
            "<" => Ok(OpLt),
            ">" => Ok(OpGt),
            "<=" => Ok(OpLte),
            ">=" => Ok(OpGte),
            "is" => Ok(OpIs),
            "like" => Ok(OpLike),
            "in" => Ok(OpIn),
            "and" => Ok(OpAnd),
            "or" => Ok(OpOr),
            _ => Err(Error::ParseError(format!("invalid unary operator {}", s))),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum UnaryOperator {
    OpNot,
}

impl FromStr for UnaryOperator {
    type Err = Error;
    fn from_str(s: &str) -> Result<UnaryOperator, Error> {
        use self::UnaryOperator::*;
        match s {
            "not" => Ok(OpNot),
            _ => Err(Error::ParseError(format!("invalid unary operator {}", s))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::BinaryOperator::*;
    use super::UnaryOperator::*;

    #[test]
    fn test_binop() {
        assert_eq!("+".parse::<BinaryOperator>().unwrap(), OpAdd);
        assert_eq!("!=".parse::<BinaryOperator>().unwrap(), OpNeq);
        assert_eq!("and".parse::<BinaryOperator>().unwrap(), OpAnd);
        assert!(OpMul.precedence() > OpAdd.precedence());
        assert!(OpMul.precedence() == OpDiv.precedence());
    }

    #[test]
    fn test_unop() {
        assert_eq!("not".parse::<UnaryOperator>().unwrap(), OpNot);
    }
}
