# SQL Parser Derive Macro

## Visit

This crate contains a procedural macro that can automatically derive
implementations of the `Visit` trait in the [sqlparser](https://crates.io/crates/sqlparser) crate

```rust
#[derive(Visit)]
struct Foo {
    boolean: bool,
    bar: Bar,
}

#[derive(Visit)]
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

Additionally certain types may wish to call a corresponding method on visitor before recursing

```rust
#[derive(Visit)]
#[visit(with = "visit_expr")]
enum Expr {
    A(),
    B(String, #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))] ObjectName, bool),
}
```

Will generate

```rust
impl Visit for Bar {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        visitor.visit_expr(self)?;
        match self {
            Self::A() => {}
            Self::B(_1, _2, _3) => {
                _1.visit(visitor)?;
                visitor.visit_relation(_3)?;
                _2.visit(visitor)?;
                _3.visit(visitor)?;
            }
        }
        ControlFlow::Continue(())
    }
}
```
