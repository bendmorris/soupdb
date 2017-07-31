use soupdb::ast::{Expr, UnaryOperator, BinaryOperator};
use soupdb::ast::value_type::ValueType;

/// To parse infix operations, tokenize everything at the same level of parens
/// then apply shunting-yard to transform into an Expr.
pub enum ExprToken {
    OpenParen,
    CloseParen,
    Term(Expr),
    UnOp(UnaryOperator),
    BinOp(BinaryOperator),
}

/// Given a vector of expression tokens, return a single compound expression.
pub fn shunting_yard(tokens: Vec<ExprToken>) -> Expr {
    let mut expr_stack: Vec<Expr> = Vec::new();
    let mut op_stack: Vec<ExprToken> = Vec::new();

    let complete_expr = |op, expr_stack: &mut Vec<Expr>| match op {
        Some(ExprToken::BinOp(op)) => {
            let rhs = expr_stack.pop().unwrap();
            let lhs = expr_stack.pop().unwrap();
            expr_stack.push(Expr::BinOp {left: Box::new(lhs), right: Box::new(rhs), op: op});
        }
        Some(ExprToken::UnOp(op)) => {
            let expr = expr_stack.pop().unwrap();
            expr_stack.push(Expr::UnOp {expr: Box::new(expr), op: op});
        }
        _ => panic!("invalid operator"),
    };

    for token in tokens {
        match token {
            ExprToken::OpenParen => {
                op_stack.push(ExprToken::OpenParen);
            }
            ExprToken::CloseParen => {
                while match op_stack.last() {
                    Some(&ExprToken::OpenParen) => false,
                    None => false,
                    _ => true,
                } {
                    complete_expr(op_stack.pop(), &mut expr_stack);
                }
                op_stack.pop();
            }
            ExprToken::Term(expr) => {
                expr_stack.push(expr);
            }
            ExprToken::UnOp(op) => {
                op_stack.push(ExprToken::UnOp(op));
            }
            ExprToken::BinOp(op) => {
                let p = op.precedence();
                while match op_stack.last() {
                    Some(&ExprToken::BinOp(ref t)) => t.precedence() >= p,
                    Some(&ExprToken::UnOp(_)) => true,
                    _ => false,
                } {
                    complete_expr(op_stack.pop(), &mut expr_stack);
                }
                op_stack.push(ExprToken::BinOp(op));
            }
        }
    }
    while op_stack.len() > 0 {
        complete_expr(op_stack.pop(), &mut expr_stack);
    }
    expr_stack.pop().unwrap()
}

#[test]
fn test_shunting_yard() {
    use self::ExprToken::{OpenParen, CloseParen, UnOp, BinOp, Term};
    let v1 = Expr::Literal {value_type: ValueType::Int, value: "1".to_string()};
    let v2 = Expr::Literal {value_type: ValueType::Int, value: "2".to_string()};
    let v3 = Expr::Literal {value_type: ValueType::Float, value: "2.5".to_string()};

    // 1
    assert_eq!(
        shunting_yard(vec![Term(v1.clone())]),
        v1.clone()
    );

    // (2)
    assert_eq!(
        shunting_yard(vec![OpenParen, Term(v2.clone()), CloseParen]),
        v2.clone()
    );

    // (1) + 2
    assert_eq!(
        shunting_yard(vec![OpenParen, Term(v1.clone()), CloseParen, BinOp(BinaryOperator::OpAdd), Term(v2.clone())]),
        Expr::BinOp {
            left: Box::new(v1.clone()),
            op: BinaryOperator::OpAdd,
            right: Box::new(v2.clone()),
        }
    );

    // 1 + (2)
    assert_eq!(
        shunting_yard(vec![Term(v1.clone()), OpenParen, BinOp(BinaryOperator::OpAdd), Term(v2.clone()), CloseParen]),
        Expr::BinOp {
            left: Box::new(v1.clone()),
            op: BinaryOperator::OpAdd,
            right: Box::new(v2.clone()),
        }
    );

    // 1 + 2
    assert_eq!(
        shunting_yard(vec![Term(v1.clone()), BinOp(BinaryOperator::OpAdd), Term(v2.clone())]),
        Expr::BinOp {
            left: Box::new(v1.clone()),
            op: BinaryOperator::OpAdd,
            right: Box::new(v2.clone()),
        }
    );

    // (1 + 2)
    assert_eq!(
        shunting_yard(vec![OpenParen, Term(v1.clone()), BinOp(BinaryOperator::OpAdd), Term(v2.clone()), CloseParen]),
        Expr::BinOp {
            left: Box::new(v1.clone()),
            op: BinaryOperator::OpAdd,
            right: Box::new(v2.clone()),
        }
    );

    // (2 - 1)
    assert_eq!(
        shunting_yard(vec![OpenParen, Term(v2.clone()), BinOp(BinaryOperator::OpSub), Term(v1.clone()), CloseParen]),
        Expr::BinOp {
            left: Box::new(v2.clone()),
            op: BinaryOperator::OpSub,
            right: Box::new(v1.clone()),
        }
    );

    // 1 + 2 * 2.5
    assert_eq!(
        shunting_yard(vec![Term(v1.clone()), BinOp(BinaryOperator::OpAdd), Term(v2.clone()), BinOp(BinaryOperator::OpMul), Term(v3.clone())]),
        Expr::BinOp {
            left: Box::new(v1.clone()),
            op: BinaryOperator::OpAdd,
            right: Box::new(Expr::BinOp {
                left: Box::new(v2.clone()),
                op: BinaryOperator::OpMul,
                right: Box::new(v3.clone()),
            }),
        }
    );

    // (1 + 2) * 2.5
    assert_eq!(
        shunting_yard(vec![OpenParen, Term(v1.clone()), BinOp(BinaryOperator::OpAdd), Term(v2.clone()), CloseParen, BinOp(BinaryOperator::OpMul), Term(v3.clone())]),
        Expr::BinOp {
            left: Box::new(Expr::BinOp {
                left: Box::new(v1.clone()),
                op: BinaryOperator::OpAdd,
                right: Box::new(v2.clone()),
            }),
            op: BinaryOperator::OpMul,
            right: Box::new(v3.clone()),
        }
    );

    // NOT 2.5
    assert_eq!(
        shunting_yard(vec![UnOp(UnaryOperator::OpNot), Term(v3.clone())]),
        Expr::UnOp {
            expr: Box::new(v3.clone()),
            op: UnaryOperator::OpNot,
        }
    );

    // 1 AND NOT 2
    assert_eq!(
        shunting_yard(vec![Term(v1.clone()), BinOp(BinaryOperator::OpAnd), UnOp(UnaryOperator::OpNot), Term(v2.clone())]),
        Expr::BinOp {
            left: Box::new(v1.clone()),
            op: BinaryOperator::OpAnd,
            right: Box::new(Expr::UnOp {
                expr: Box::new(v2.clone()),
                op: UnaryOperator::OpNot,
            }),
        }
    );

    // NOT 1 AND 2
    assert_eq!(
        shunting_yard(vec![UnOp(UnaryOperator::OpNot), Term(v1.clone()), BinOp(BinaryOperator::OpAnd), Term(v2.clone())]),
        Expr::BinOp {
            left: Box::new(Expr::UnOp {
                expr: Box::new(v1.clone()),
                op: UnaryOperator::OpNot,
            }),
            op: BinaryOperator::OpAnd,
            right: Box::new(v2.clone()),
        }
    );
}
