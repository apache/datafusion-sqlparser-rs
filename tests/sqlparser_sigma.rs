#![warn(clippy::all)]

use sqlparser::ast::*;
use sqlparser::dialect::SnowflakeDialect;
use test_utils::*;

#[macro_use]
mod test_utils;

fn snowflake() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {})],
        options: None,
    }
}
#[test]
fn parse_sigma() {
    let sql = "SELECT my_column FROM @sigma.my_element";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        select.from,
        vec![TableWithJoins {
            relation: TableFactor::SigmaElement {
                element: Ident::new("my_element"),
                alias: None
            },
            joins: vec![]
        }]
    )
}
