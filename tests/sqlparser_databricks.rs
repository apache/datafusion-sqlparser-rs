use sqlparser::ast::*;
use sqlparser::dialect::DatabricksDialect;
use sqlparser::parser::ParserError;
use test_utils::*;

#[macro_use]
mod test_utils;

fn databricks() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DatabricksDialect {})],
        options: None,
    }
}

#[test]
fn test_databricks_identifiers() {
    // databricks uses backtick for delimited identifiers
    assert_eq!(
        databricks().verified_only_select("SELECT `Ä`").projection[0],
        SelectItem::UnnamedExpr(Expr::Identifier(Ident::with_quote('`', "Ä")))
    );

    // double quotes produce string literals, not delimited identifiers
    assert_eq!(
        databricks()
            .verified_only_select(r#"SELECT "Ä""#)
            .projection[0],
        SelectItem::UnnamedExpr(Expr::Value(Value::DoubleQuotedString("Ä".to_owned())))
    );
}

#[test]
fn test_databricks_exists() {
    // exists is a function in databricks
    assert_eq!(
        databricks().verified_expr("exists(array(1, 2, 3), x -> x IS NULL)"),
        call(
            "exists",
            [
                call(
                    "array",
                    [
                        Expr::Value(number("1")),
                        Expr::Value(number("2")),
                        Expr::Value(number("3"))
                    ]
                ),
                Expr::Lambda(LambdaFunction {
                    params: OneOrManyWithParens::One(Ident::new("x")),
                    body: Box::new(Expr::IsNull(Box::new(Expr::Identifier(Ident::new("x")))))
                })
            ]
        ),
    );

    let res = databricks().parse_sql_statements("SELECT EXISTS (");
    assert_eq!(
        // TODO: improve this error message...
        ParserError::ParserError("Expected an expression:, found: EOF".to_string()),
        res.unwrap_err(),
    );
}

#[test]
fn test_databricks_lambdas() {
    #[rustfmt::skip]
    let sql = concat!(
        "SELECT array_sort(array('Hello', 'World'), ",
            "(p1, p2) -> CASE WHEN p1 = p2 THEN 0 ",
                        "WHEN reverse(p1) < reverse(p2) THEN -1 ",
                        "ELSE 1 END)",
    );
    pretty_assertions::assert_eq!(
        SelectItem::UnnamedExpr(call(
            "array_sort",
            [
                call(
                    "array",
                    [
                        Expr::Value(Value::SingleQuotedString("Hello".to_owned())),
                        Expr::Value(Value::SingleQuotedString("World".to_owned()))
                    ]
                ),
                Expr::Lambda(LambdaFunction {
                    params: OneOrManyWithParens::Many(vec![Ident::new("p1"), Ident::new("p2")]),
                    body: Box::new(Expr::Case {
                        operand: None,
                        conditions: vec![
                            Expr::BinaryOp {
                                left: Box::new(Expr::Identifier(Ident::new("p1"))),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Identifier(Ident::new("p2")))
                            },
                            Expr::BinaryOp {
                                left: Box::new(call(
                                    "reverse",
                                    [Expr::Identifier(Ident::new("p1"))]
                                )),
                                op: BinaryOperator::Lt,
                                right: Box::new(call(
                                    "reverse",
                                    [Expr::Identifier(Ident::new("p2"))]
                                ))
                            }
                        ],
                        results: vec![
                            Expr::Value(number("0")),
                            Expr::UnaryOp {
                                op: UnaryOperator::Minus,
                                expr: Box::new(Expr::Value(number("1")))
                            }
                        ],
                        else_result: Some(Box::new(Expr::Value(number("1"))))
                    })
                })
            ]
        )),
        databricks().verified_only_select(sql).projection[0]
    );

    databricks().verified_expr(
        "map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2))",
    );
    databricks().verified_expr("transform(array(1, 2, 3), x -> x + 1)");
}
