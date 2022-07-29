# SQL Lexer, Parser, AST, and Dialect-Aware SQL Generator for Rust
This project is a fork of the excellent [`sqlparser-rs`](https://github.com/sqlparser-rs/sqlparser-rs) project.

Whereas `sqlparser-rs` aims to parse SQL in a variety of dialects, `sqlgen-rs` aims to generate SQL query strings in a variety of SQL dialects. The goal of the project is to make it possible to define SQL queries in a single generic dialect and then translate them into a variety of custom dialects.

Dialect customization has not yet been implemented

## Example

To parse a simple `SELECT` statement:

```rust
use sqlgen::parser::Parser;

let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

let ast = Parser::parse_sql_query(sql).unwrap();

println!("AST: {:?}", ast);
```

This outputs

```
AST: Query { with: None, body: Select(Select { distinct: false, top: None, projection: [UnnamedExpr(Identifier(Ident { value: "a", quote_style: None })), UnnamedExpr(Identifier(Ident { value: "b", quote_style: None })), UnnamedExpr(Value(Number("123", false))), UnnamedExpr(Function(Function { name: ObjectName([Ident { value: "myfunc", quote_style: None }]), args: [Unnamed(Expr(Identifier(Ident { value: "b", quote_style: None })))], over: None, distinct: false }))], into: None, from: [TableWithJoins { relation: Table { name: ObjectName([Ident { value: "table_1", quote_style: None }]), alias: None, args: None, with_hints: [] }, joins: [] }], lateral_views: [], selection: Some(BinaryOp { left: BinaryOp { left: Identifier(Ident { value: "a", quote_style: None }), op: Gt, right: Identifier(Ident { value: "b", quote_style: None }) }, op: And, right: BinaryOp { left: Identifier(Ident { value: "b", quote_style: None }), op: Lt, right: Value(Number("100", false)) } }), group_by: [], cluster_by: [], distribute_by: [], sort_by: [], having: None, qualify: None }), order_by: [OrderByExpr { expr: Identifier(Ident { value: "a", quote_style: None }), asc: Some(false), nulls_first: None }, OrderByExpr { expr: Identifier(Ident { value: "b", quote_style: None }), asc: None, nulls_first: None }], limit: None, offset: None, fetch: None, lock: None }
```

## Command line
To parse a file and dump the results as JSON:
```
$ cargo run --features json_example --example cli FILENAME.sql [--dialectname]
```
