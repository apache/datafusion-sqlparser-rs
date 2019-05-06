#![warn(clippy::all)]
//! Test SQL syntax specific to Microsoft's T-SQL. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

use sqlparser::dialect::{GenericSqlDialect, MsSqlDialect};
use sqlparser::sqlast::*;
use sqlparser::test_utils::*;

#[test]
fn parse_mssql_identifiers() {
    let sql = "SELECT @@version, _foo$123 FROM ##temp";
    let select = ms_and_generic().verified_only_select(sql);
    assert_eq!(
        &ASTNode::SQLIdentifier("@@version".to_string()),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &ASTNode::SQLIdentifier("_foo$123".to_string()),
        expr_from_projection(&select.projection[1]),
    );
    assert_eq!(2, select.projection.len());
    match select.relation {
        Some(TableFactor::Table { name, .. }) => {
            assert_eq!("##temp".to_string(), name.to_string());
        }
        _ => unreachable!(),
    };
}

#[allow(dead_code)]
fn ms() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(MsSqlDialect {})],
    }
}
fn ms_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(MsSqlDialect {}), Box::new(GenericSqlDialect {})],
    }
}
