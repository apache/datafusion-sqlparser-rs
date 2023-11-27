# SQL Parser Derive Macro

## Visit

This crate contains a procedural macro that can automatically derive
implementations of the `Visit` trait in the [sqlparser](https://crates.io/crates/sqlparser) crate

```rust
#[derive(Visit, VisitMut)]
struct Foo {
    boolean: bool,
    bar: Bar,
}

#[derive(Visit, VisitMut)]
enum Bar {
    A(),
    B(String, bool),
    C { named: i32 },
}
```

Will generate code akin to

```rust
impl Visit for Foo {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        self.boolean.visit(visitor)?;
        self.bar.visit(visitor)?;
        ControlFlow::Continue(())
    }
}

impl Visit for Bar {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        match self {
            Self::A() => {}
            Self::B(_1, _2) => {
                _1.visit(visitor)?;
                _2.visit(visitor)?;
            }
            Self::C { named } => {
                named.visit(visitor)?;
            }
        }
        ControlFlow::Continue(())
    }
}
```

Some types may wish to call a corresponding method on the visitor:

```rust
#[derive(Visit, VisitMut)]
#[visit(with = "visit_expr")]
enum Expr {
    IsNull(Box<Expr>),
    ..
}
```

This will result in the following sequence of visitor calls when an `IsNull`
expression is visited

```
visitor.pre_visit_expr(<is null expr>)
visitor.pre_visit_expr(<is null operand>)
visitor.post_visit_expr(<is null operand>)
visitor.post_visit_expr(<is null expr>)
```

For some types it is only appropriate to call a particular visitor method in
some contexts. For example, not every `ObjectName` refers to a relation.

In these cases, the `visit` attribute can be used on the field for which we'd
like to call the method:

```rust
#[derive(Visit, VisitMut)]
#[visit(with = "visit_table_factor")]
pub enum TableFactor {
    Table {
        #[visit(with = "visit_relation")]
        name: ObjectName,
        alias: Option<TableAlias>,
    },
    ..
}
```

This will generate

```rust
impl Visit for TableFactor {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        visitor.pre_visit_table_factor(self)?;
        match self {
            Self::Table { name, alias } => {
                visitor.pre_visit_relation(name)?;
                alias.visit(name)?;
                visitor.post_visit_relation(name)?;
                alias.visit(visitor)?;
            }
        }
        visitor.post_visit_table_factor(self)?;
        ControlFlow::Continue(())
    }
}
```

Note that annotating both the type and the field is incorrect as it will result
in redundant calls to the method. For example

```rust
#[derive(Visit, VisitMut)]
#[visit(with = "visit_expr")]
enum Expr {
    IsNull(#[visit(with = "visit_expr")] Box<Expr>),
    ..
}
```

will result in these calls to the visitor


```
visitor.pre_visit_expr(<is null expr>)
visitor.pre_visit_expr(<is null operand>)
visitor.pre_visit_expr(<is null operand>)
visitor.post_visit_expr(<is null operand>)
visitor.post_visit_expr(<is null operand>)
visitor.post_visit_expr(<is null expr>)
```

## Releasing

This crate's release is not automated. Instead it is released manually as needed

Steps:
1. Update the version in `Cargo.toml`
2. Update the corresponding version in `../Cargo.toml`
3. Commit via PR
4. Publish to crates.io:

```shell
# update to latest checked in main branch and publish via
cargo publish 
```

