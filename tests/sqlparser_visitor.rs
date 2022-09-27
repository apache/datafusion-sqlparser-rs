#[cfg(feature = "derive-visitor")]
mod test {
    use derive_visitor::{visitor_enter_fn_mut, Drive, DriveMut, Visitor};
    use sqlparser::ast;
    use sqlparser::ast::TableFactor::Table;
    use sqlparser::ast::{
        Ident, Join, JoinConstraint, JoinOperator, ObjectName, TableFactor, Value,
    };
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

    #[test]
    fn test_visitor_add_table_to_join() {
        let sql = "select a,b from t1";
        // Table t1 has changed and its fields are now split between tables t1 and t2
        let mut ast = Parser::parse_sql(&GenericDialect, sql).unwrap();
        ast[0].drive_mut(&mut visitor_enter_fn_mut(
            |table: &mut ast::TableWithJoins| {
                let has_t1 = std::iter::once(&table.relation)
                    .chain(table.joins.iter().map(|j| &j.relation))
                    .any(|r| {
                        matches!(r, TableFactor::Table {name: ObjectName(idents), ..}
                        if idents[0].value == "t1")
                    });
                if has_t1 {
                    table.joins.push(Join {
                        relation: Table {
                            name: ObjectName(vec![Ident::from("t2")]),
                            alias: None,
                            args: None,
                            with_hints: vec![],
                        },
                        join_operator: JoinOperator::Inner(JoinConstraint::Using(vec![
                            Ident::from("t_id"),
                        ])),
                    });
                }
            },
        ));
        assert_eq!(
            ast[0].to_string(),
            "SELECT a, b FROM t1 JOIN t2 USING(t_id)"
        );
    }

    #[test]
    fn test_immutable_visitor_count_parameters() {
        let sql = "select a, b+$x from t1 where c=$y";
        // Table t1 has changed and its fields are now split between tables t1 and t2
        let ast = Parser::parse_sql(&GenericDialect, sql).unwrap();

        /// Counts the placeholder in an SQL statement
        #[derive(Visitor, Default)]
        #[visitor(Value(enter))]
        struct PlaceholderCounter(usize);
        impl PlaceholderCounter {
            fn enter_value(&mut self, value: &Value) {
                if let Value::Placeholder(_) = value {
                    self.0 += 1;
                }
            }
        }

        let mut counter = PlaceholderCounter(0);
        ast[0].drive(&mut counter);
        assert_eq!(counter.0, 2, "There are 2 placeholders in the query");
    }

    #[test]
    fn test_readme_example() {
        let mut statements = Parser::parse_sql(&GenericDialect, "select xxx").unwrap();
        statements[0].drive_mut(&mut visitor_enter_fn_mut(|ident: &mut Ident| {
            ident.value = ident.value.replace("xxx", "yyy");
        }));
        assert_eq!(statements[0].to_string(), "SELECT yyy");
    }
}
