use sqlparser::ast::*;
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::ParserError;
use sqlparser::test_utils::*;

fn table_alias(alias: &str) -> TableAlias {
    TableAlias {
        name: Ident {
            value: alias.to_owned(),
            quote_style: None,
        },
        columns: Vec::new(),
    }
}

fn table(name: impl Into<String>, alias: Option<TableAlias>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName(vec![Ident::new(name.into())]),
        alias,
        args: vec![],
        with_hints: vec![],
    }
}

fn join(relation: TableFactor) -> Join {
    Join {
        relation,
        join_operator: JoinOperator::Inner(JoinConstraint::Natural),
    }
}

macro_rules! nest {
    ($base:expr $(, $join:expr)*) => {
        TableFactor::NestedJoin(Box::new(TableWithJoins {
            relation: $base,
            joins: vec![$(join($join)),*]
        }))
    };
}

fn sf() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {})],
    }
}

fn get_from_section_from_select_query(query: &str) -> Vec<TableWithJoins> {
    let statement = sf().parse_sql_statements(query).unwrap()[0].clone();

    let query = match statement {
        Statement::Query(query) => query,
        _ => panic!("Not a query"),
    };

    let select = match query.body {
        SetExpr::Select(select) => select,
        _ => panic!("not a select query"),
    };

    select.from.clone()
}

#[test]
fn test_sf_derives_single_table_in_parenthesis() {
    let from = get_from_section_from_select_query("SELECT * FROM (((SELECT 1) AS t))");

    assert_eq!(
        from[0].relation,
        TableFactor::Derived {
            lateral: false,
            subquery: Box::new(sf().verified_query("SELECT 1")),
            alias: Some(TableAlias {
                name: "t".into(),
                columns: vec![],
            })
        }
    );
}

#[test]
fn test_single_table_in_parenthesis() {
    //Parenthesized table names are non-standard, but supported in Snowflake SQL
    let from = get_from_section_from_select_query("SELECT * FROM (a NATURAL JOIN (b))");

    assert_eq!(from[0].relation, nest!(table("a", None), table("b", None)));

    let from = get_from_section_from_select_query("SELECT * FROM (a NATURAL JOIN ((b)))");
    assert_eq!(from[0].relation, nest!(table("a", None), table("b", None)));
}

#[test]
fn test_single_table_in_parenthesis_with_alias() {
    let sql = "SELECT * FROM (a NATURAL JOIN (b) c )";
    let table_with_joins = get_from_section_from_select_query(sql)[0].clone();
    assert_eq!(
        table_with_joins.relation,
        nest!(table("a", None), table("b", Some(table_alias("c"))))
    );

    let sql = "SELECT * FROM (a NATURAL JOIN ((b)) c )";
    let table_with_joins = get_from_section_from_select_query(sql)[0].clone();
    assert_eq!(
        table_with_joins.relation,
        nest!(table("a", None), table("b", Some(table_alias("c"))))
    );

    let sql = "SELECT * FROM (a NATURAL JOIN ( (b) c ) )";
    let table_with_joins = get_from_section_from_select_query(sql)[0].clone();
    assert_eq!(
        table_with_joins.relation,
        nest!(table("a", None), table("b", Some(table_alias("c"))))
    );

    let sql = "SELECT * FROM (a NATURAL JOIN ( (b) as c ) )";
    let table_with_joins = get_from_section_from_select_query(sql)[0].clone();
    assert_eq!(
        table_with_joins.relation,
        nest!(table("a", None), table("b", Some(table_alias("c"))))
    );

    let sql = "SELECT * FROM (a alias1 NATURAL JOIN ( (b) c ) )";
    let table_with_joins = get_from_section_from_select_query(sql)[0].clone();
    assert_eq!(
        table_with_joins.relation,
        nest!(
            table("a", Some(table_alias("alias1"))),
            table("b", Some(table_alias("c")))
        )
    );

    let sql = "SELECT * FROM (a as alias1 NATURAL JOIN ( (b) as c ) )";
    let table_with_joins = get_from_section_from_select_query(sql)[0].clone();
    assert_eq!(
        table_with_joins.relation,
        nest!(
            table("a", Some(table_alias("alias1"))),
            table("b", Some(table_alias("c")))
        )
    );

    let res = sf().parse_sql_statements("SELECT * FROM (a NATURAL JOIN b) c");
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: c".to_string()),
        res.unwrap_err()
    );

    let res = sf().parse_sql_statements("SELECT * FROM (a b) c");
    assert_eq!(
        ParserError::ParserError("duplicate alias b".to_string()),
        res.unwrap_err()
    );
}
