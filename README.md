# SQL Parser

The goal of this project is to build a SQL lexer and parser capable of parsing ANSI SQL:2011 (or 2016 if I can get access to the specification for free).

The current code is capable of parsing some simple SELECT and CREATE TABLE statements.

```rust
let sql = "SELECT a, b, 123, myfunc(b) \
    FROM table_1 \
    WHERE a > b AND b < 100 \
    ORDER BY a DESC, b";

let ast = Parser::parse_sql(sql.to_string()).unwrap();

println!("AST: {:?}", ast);
```

This outputs

```rust
AST: SQLSelect { projection: [SQLIdentifier("a"), SQLIdentifier("b"), SQLLiteralLong(123), SQLFunction { id: "myfunc", args: [SQLIdentifier("b")] }], relation: Some(SQLIdentifier("table_1")), selection: Some(SQLBinaryExpr { left: SQLBinaryExpr { left: SQLIdentifier("a"), op: Gt, right: SQLIdentifier("b") }, op: And, right: SQLBinaryExpr { left: SQLIdentifier("b"), op: Lt, right: SQLLiteralLong(100) } }), order_by: Some([SQLOrderBy { expr: SQLIdentifier("a"), asc: false }, SQLOrderBy { expr: SQLIdentifier("b"), asc: true }]), group_by: None, having: None, limit: None }
```