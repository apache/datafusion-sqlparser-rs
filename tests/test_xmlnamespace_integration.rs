/// Test to verify XMLNAMESPACES parsing and AST storage
/// This demonstrates that the XMLNAMESPACES clause is now properly stored in the AST
/// instead of being dropped.
use sqlparser::ast::Statement;
use sqlparser::dialect::MsSqlDialect;
use sqlparser::parser::Parser;

#[test]
fn test_xmlnamespaces_parsing_and_ast_storage() {
    let dialect = MsSqlDialect {};
    let sql = r#"
        WITH XMLNAMESPACES ('http://example.com' AS ex, 'http://other.com' AS ot)
        SELECT 1 AS col
    "#;

    let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    let ast = parser.parse_statements().unwrap();

    assert_eq!(ast.len(), 1, "Should parse as a single statement");

    match &ast[0] {
        Statement::Query(query) => {
            // Verify the WITH clause is present
            assert!(query.with.is_some(), "Query should have WITH clause");

            let with_clause = query.with.as_ref().unwrap();

            // Verify xml_namespaces were captured
            assert_eq!(
                with_clause.xml_namespaces.len(),
                2,
                "Should have 2 XML namespace definitions"
            );

            // Check first namespace
            let first_ns = &with_clause.xml_namespaces[0];
            assert_eq!(
                first_ns.name.value, "ex",
                "First namespace alias should be 'ex'"
            );

            // Check second namespace
            let second_ns = &with_clause.xml_namespaces[1];
            assert_eq!(
                second_ns.name.value, "ot",
                "Second namespace alias should be 'ot'"
            );

            // Verify CTEs are empty (no CTEs after XMLNAMESPACES in this example)
            assert_eq!(with_clause.cte_tables.len(), 0, "Should have no CTE tables");

            // Verify Display output includes XMLNAMESPACES
            let display_output = format!("{}", with_clause);
            assert!(
                display_output.contains("XMLNAMESPACES"),
                "Display output should include XMLNAMESPACES"
            );

            println!("✓ XMLNAMESPACES AST representation: {}", display_output);
        }
        _ => panic!("Expected Query statement"),
    }
}

#[test]
fn test_xmlnamespaces_with_ctes() {
    let dialect = MsSqlDialect {};
    let sql = r#"
        WITH XMLNAMESPACES ('http://example.com' AS ex),
             cte1 AS (SELECT 1 AS col)
        SELECT * FROM cte1
    "#;

    let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    let ast = parser.parse_statements().unwrap();

    assert_eq!(ast.len(), 1, "Should parse as a single statement");

    match &ast[0] {
        Statement::Query(query) => {
            let with_clause = query.with.as_ref().unwrap();

            // Verify namespaces
            assert_eq!(
                with_clause.xml_namespaces.len(),
                1,
                "Should have 1 XML namespace definition"
            );

            // Verify CTEs
            assert_eq!(with_clause.cte_tables.len(), 1, "Should have 1 CTE table");
            assert_eq!(
                with_clause.cte_tables[0].alias.name.value, "cte1",
                "CTE name should be 'cte1'"
            );

            let display_output = format!("{}", with_clause);
            println!("✓ XMLNAMESPACES with CTEs: {}", display_output);
            assert!(display_output.contains("XMLNAMESPACES"));
            assert!(display_output.contains("cte1"));
        }
        _ => panic!("Expected Query statement"),
    }
}

#[test]
fn test_xmlnamespaces_display_format() {
    let dialect = MsSqlDialect {};
    let sql = r#"
        WITH XMLNAMESPACES ('http://example.com' AS ex, 'http://other.com' AS ot),
             my_cte AS (SELECT 1)
        SELECT * FROM my_cte
    "#;

    let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
    let ast = parser.parse_statements().unwrap();

    match &ast[0] {
        Statement::Query(query) => {
            let with_clause = query.with.as_ref().unwrap();
            let display_output = format!("{}", with_clause);

            // Verify the order: XMLNAMESPACES comes first, then CTEs
            assert!(
                display_output.starts_with("WITH XMLNAMESPACES"),
                "Display should start with 'WITH XMLNAMESPACES'"
            );

            println!("✓ Full display format: {}", display_output);
        }
        _ => panic!("Expected Query statement"),
    }
}
