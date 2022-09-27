#[cfg(feature = "derive-visitor")]
mod test {
    use derive_visitor::{visitor_enter_fn_mut, DriveMut};
    use sqlparser::ast::TableFactor::Table;
    use sqlparser::ast::{ObjectName, TableFactor};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test_visitor() {
        let sql = "SELECT x FROM table_a JOIN table_b";
        let mut ast = Parser::parse_sql(&GenericDialect, sql).unwrap();
        ast.drive_mut(&mut visitor_enter_fn_mut(|table: &mut TableFactor| {
            if let Table {
                name: ObjectName(parts),
                ..
            } = table
            {
                parts[0].value = parts[0].value.replace("table_", "");
            }
        }));
        assert_eq!(ast[0].to_string(), "SELECT x FROM a JOIN b");
    }
}
