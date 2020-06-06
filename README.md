> _The following pertains to the `cst` branch; the [upstream README is below](#upstream-readme)._
>
> ⚠️ This branch is regularly rebased. Please let me know before working off it to coordinate.

**Preserving full source code information ([#161](https://github.com/andygrove/sqlparser-rs/issues/161)) would enable SQL rewriting/refactoring tools based on sqlparser-rs.** For example:
1. **Error reporting**, both in the parser and in later stages of query processing,  would benefit from knowing the source code location of SQL constructs ([#179](https://github.com/andygrove/sqlparser-rs/issues/179))
2. **SQL pretty-printing** requires comments to be preserved in AST (see [#175](https://github.com/andygrove/sqlparser-rs/issues/175), mentioning [forma](https://github.com/maxcountryman/forma))
3. **Refactoring via AST transformations** would also benefit from having full control over serialization, a possible solution for dialect-specific "writers" ([#18](https://github.com/andygrove/sqlparser-rs/issues/18))
4. Analyzing partially invalid code may be useful in the context of an IDE or other tooling.

**I think that adopting [rust-analyzer's design][ra-syntax], that includes a lossless syntax tree, is the right direction for sqlparser-rs.** In addition to solving the use-cases described above, it helps in other ways:

5. We can omit syntax that does not affect semantics of the query (e.g. [`ROW` vs `ROWS`](https://github.com/andygrove/sqlparser-rs/blob/418b9631ce9c24cf9bb26cf7dd9e42edd29de985/src/ast/query.rs#L416)) from the typed AST by default, reducing the implementation effort.
6. Having a homogenous syntax tree also alleviates the need for a "visitor" ([#114](https://github.com/andygrove/sqlparser-rs/pull/114)), also reducing the burden on implementors of new syntax

In 2020 many new people contributed to `sqlparser-rs`, some bringing up the use-cases above. I found myself mentioning this design multiple times, so I felt I should "show the code" instead of just talking about it.

Current typed AST vs rowan
==========================

To recap, the input SQL is currently parsed directly into _typed AST_ - with each node of the tree represented by a Rust `struct`/`enum` of a specific type, referring to other structs of specific type, such as:

    struct Select {
        pub projection: Vec<SelectItem>,
        ...
    }

We try to retain most of "important" syntax in this representation (including, for example, [`Ident::quote_style`](https://github.com/andygrove/sqlparser-rs/blob/d32df527e68dd76d857f47ea051a3ec22138469b/src/ast/mod.rs#L77) and [`OffsetRows`](https://github.com/andygrove/sqlparser-rs/blob/418b9631ce9c24cf9bb26cf7dd9e42edd29de985/src/ast/query.rs#L416)), but there doesn't seem to be a practical way to extend it to also store whitespace, comments, and source code spans.

The lossless syntax tree
------------------------

In the alternative design, the parser produces a tree (which I'll call "CST", [not 100% correct though it is](https://dev.to/cad97/lossless-syntax-trees-280c)), in which every node has has the same Rust type (`SyntaxNode`), and a numeric `SyntaxKind` determines what kind of node it is. Under the hood, the leaf and the non-leaf nodes are different:

* Each leaf node stores a slice of the source text;
* Each intermediate node represents a string obtained by concatenatenating the text of its children;
* The root node, consequently, represents exactly the original source code.

_(The actual [rust-analyzer's design][ra-syntax] is more involved, but the details are not relevant to this discussion.)_

As an example, an SQL query "`select DISTINCT /* ? */ 1234`" could be represented as a tree like the following one:

    SELECT@0..29
      Keyword@0..6                "select"
      Whitespace@6..7             " "
      DISTINCT_OR_ALL
        Keyword@7..15             "DISTINCT"
      SELECT_ITEM_UNNAMED@16..29
        Whitespace@16..17         " "
        Comment@17..24            "/* ? */"
        Whitespace@24..25         " "
        Number@25..29             "1234"

_(Using the `SyntaxKind@start_pos..end_pos   "relevant source code"` notation)_

Note how all the formatting and comments are retained.

Such tree data structure is available for re-use as a separate crate ([`rowan`](https://github.com/rust-analyzer/rowan)), and **as the proof-of-concept I extended the parser to populate a rowan-based tree _along with the typed AST_, for a few of the supported SQL constructs.**

Open question: The future design of the typed AST
-------------------------------------------------

Though included in the PoC, **constructing both an AST and a CST in parallel should be considered a transitional solution only**, as it will not let us reap the full benefits of the proposed design (esp. points 1, 4, and 5). Additionally, the current one-AST-fits-all approach makes every new feature in the parser a (semver) breaking change, and makes the types as loose as necessary to fit the common denominator (e.g. if something is optional in one dialect, it has to be optional in the AST).

What can we do instead?

### Rust-analyzer's AST

In rust-analyzer the AST layer does not store any additional data. Instead a "newtype" (a struct with exactly one field - the underlying CST node) is defined for each AST node type:

    struct Select { syntax: SyntaxNode };  // the newtype
    impl Select {
        fn syntax(&self) -> &SyntaxNode { &self.syntax }
        fn cast(syntax: SyntaxNode) -> Option<Self> {
            match syntax.kind {
                SyntaxKind::SELECT => Some(Select { syntax }),
                _ => None,
            }
        }
        ...

Such newtypes define APIs to let the user navigate the AST through accessors specific to this node type:

        // ...`impl Select` continued
        pub fn distinct_or_all(&self) -> Option<DistinctOrAll> {
            AstChildren::new(&self.syntax).next()
        }
        pub fn projection(&self) -> AstChildren<SelectItem> {
            AstChildren::new(&self.syntax)
        }

These accessors go through the node's direct childen, looking for nodes of a specific `SyntaxKind` (by trying to `cast()` them to the requested output type).

This approach is a good fit for IDEs, as it can work on partial / invalid source code due to its lazy nature. Whether it is acceptable in other contexts is an open question ([though it was **not** rejected w.r.t rust-analyzer and rustc sharing a libsyntax2.0](https://github.com/rust-lang/rfcs/pull/2256)).

### Code generation and other options for the AST

Though the specific form of the AST is yet to be determined, it seems necessary to use some form of automation to build an AST based on a CST, so that we don't have 3 places (the parser, the AST, and the CST->AST converter) to keep synchronised.

Rust-analyzer implements its own simple code generator, which would [generate](https://github.com/rust-analyzer/rust-analyzer/blob/a0be39296d2925972cacd9fbf8b5fb258fad6947/xtask/src/codegen/gen_syntax.rs#L47) methods like the above based on a definition [like](https://github.com/rust-analyzer/rust-analyzer/blob/a0be39296d2925972cacd9fbf8b5fb258fad6947/xtask/src/ast_src.rs#L293) this:

    const AST_SRC: AstSrc = AstSrc {
        nodes: &ast_nodes! {
            struct Select {
                DistinctOrAll,
                projection: [SelectItem],
                ...

_(Here the `ast_nodes!` macro converts something that looks like a simplified `struct` declaration to a literal value describing the struct's name and fields.)_

A similar approach could be tried to eagerly build an AST akin to our current one [[*](#ref-1)]. A quick survey of our AST reveals some incompatibilities between the rust-analyzer-style codegen and our use-case:

* In rust-analyzer all AST enums use fieldless variants (`enum Foo { Bar(Bar), Baz(Baz) }`), making codegen easier. sqlparser uses variants with fields, though there was a request to move to fieldless ([#40](https://github.com/andygrove/sqlparser-rs/issues/40)).

    In my view, our motivation here was conciseness and inertia (little reason to rewrite, and much effort needed to update - both the library, including the tests, and the consumers). I think this can change.

* RA's codegen assumes that the _type_ of a node usually determines its relation to its parent: different fields in a code-generated struct have to be of different types, as all children of a given type are available from a single "field". Odd cases like `BinExpr`'s `lhs` and `rhs` (both having the type `Expr`) are [implemented manually](https://github.com/rust-analyzer/rust-analyzer/blob/a0be39296d2925972cacd9fbf8b5fb258fad6947/crates/ra_syntax/src/ast/expr_extensions.rs#L195).

    It's clear this does not work as well for an AST like ours. In rare cases there's a clear problem with our AST (e.g. `WhenClause` for `CASE` expressions is introduced on this branch), but consider:

        pub struct Select {
            //...
            pub projection: Vec<SelectItem>,
            pub where: Option<Expr>,
            pub group_by: Vec<Expr>,
            pub having: Option<Expr>,

    The CST for this should probably have separate branches for `WHERE`, `GROUP BY` and `HAVING` at least. Should we introduce additional types like `Where` or make codegen handle this somehow?

* Large portions of the `struct`s in our AST are allocated at once. We use `Box` only where necessary to break the cycles. RA's codegen doesn't have a way to specify these points.

Of course we're not limited to stealing ideas from rust-analyzer, so alternatives can be considered.

* Should we code-gen based on a real AST definition instead of a quasi-Rust code inside a macro like `ast_nodes`?
* Can `serde` be of help?

I think the design of the CST should be informed by the needs of the AST, so **this is the key question for me.** I've extracted the types and the fields of the current AST into a table (see `ast-stats.js` and `ast-fields.tsv` in `util/`) to help come up with a solution.


Other tasks
-----------

Other than coming up with AST/CST design, there are a number of things to do:

* Upstream the "Support parser backtracking in the GreenNodeBuilder" commit to avoid importing a copy of `GreenNodeBuilder` into sqlparser-rs
* Setting up the testing infrastructure for the CST (rust-analyzer, again, has some good ideas here)

<!-- the following is copied from the
    "Introduce CST infrastructure based on rowan" commit -->
- Fix `Token`/`SyntaxKind` duplication, changing the former to
    store a slice of the original source code, e.g.
    `(SyntaxKind, SmolStr)`

  This should also fix the currently disabled test-cases where `Token`'s
  `to_string()` does not return the original string:

    * `parse_escaped_single_quote_string_predicate`
    * `parse_literal_string`  (the case of `HexLiteralString`)

- Fix the hack in `parse_keyword()` to remap the token type (RA has
  `bump_remap() for this)

- Fix the `Parser::pending` hack (needs rethinking parser's API)

    Probably related is the issue of handling whitespace and comments:
    the way this prototype handles it looks wrong.

Remarks
-------

1. <a name="ref-1">[*]</a> During such eager construction of an AST we could also bail on CST nodes that have no place in the typed AST. This seems a part of the possible solution to the dialect problem: this way the parser can recognize a dialect-specific construct, while each consumer can pick which bits they want to support by defining their own typed AST.


[ra-syntax]: https://github.com/rust-analyzer/rust-analyzer/blob/master/docs/dev/syntax.md









# Upstream README

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/sqlparser.svg)](https://crates.io/crates/sqlparser)
[![Build Status](https://travis-ci.org/andygrove/sqlparser-rs.svg?branch=master)](https://travis-ci.org/andygrove/sqlparser-rs)
[![Coverage Status](https://coveralls.io/repos/github/andygrove/sqlparser-rs/badge.svg?branch=master)](https://coveralls.io/github/andygrove/sqlparser-rs?branch=master)
[![Gitter Chat](https://badges.gitter.im/sqlparser-rs/community.svg)](https://gitter.im/sqlparser-rs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

The goal of this project is to build a SQL lexer and parser capable of parsing
SQL that conforms with the [ANSI/ISO SQL standard][sql-standard] while also
making it easy to support custom dialects so that this crate can be used as a
foundation for vendor-specific parsers.

This parser is currently being used by the [DataFusion] query engine and
[LocustDB].

## Example

To parse a simple `SELECT` statement:

```rust
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

let ast = Parser::parse_sql(&dialect, sql).unwrap();

println!("AST: {:?}", ast);
```

This outputs

```rust
AST: [Query(Query { ctes: [], body: Select(Select { distinct: false, projection: [UnnamedExpr(Identifier("a")), UnnamedExpr(Identifier("b")), UnnamedExpr(Value(Long(123))), UnnamedExpr(Function(Function { name: ObjectName(["myfunc"]), args: [Identifier("b")], over: None, distinct: false }))], from: [TableWithJoins { relation: Table { name: ObjectName(["table_1"]), alias: None, args: [], with_hints: [] }, joins: [] }], selection: Some(BinaryOp { left: BinaryOp { left: Identifier("a"), op: Gt, right: Identifier("b") }, op: And, right: BinaryOp { left: Identifier("b"), op: Lt, right: Value(Long(100)) } }), group_by: [], having: None }), order_by: [OrderByExpr { expr: Identifier("a"), asc: Some(false) }, OrderByExpr { expr: Identifier("b"), asc: None }], limit: None, offset: None, fetch: None })]
```

## SQL compliance

SQL was first standardized in 1987, and revisions of the standard have been
published regularly since. Most revisions have added significant new features to
the language, and as a result no database claims to support the full breadth of
features. This parser currently supports most of the SQL-92 syntax, plus some
syntax from newer versions that have been explicitly requested, plus some MSSQL,
PostgreSQL, and other dialect-specific syntax. Whenever possible, the [online
SQL:2016 grammar][sql-2016-grammar] is used to guide what syntax to accept.

Unfortunately, stating anything more specific about compliance is difficult.
There is no publicly available test suite that can assess compliance
automatically, and doing so manually would strain the project's limited
resources. Still, we are interested in eventually supporting the full SQL
dialect, and we are slowly building out our own test suite.

If you are assessing whether this project will be suitable for your needs,
you'll likely need to experimentally verify whether it supports the subset of
SQL that you need. Please file issues about any unsupported queries that you
discover. Doing so helps us prioritize support for the portions of the standard
that are actually used. Note that if you urgently need support for a feature,
you will likely need to write the implementation yourself. See the
[Contributing](#Contributing) section for details.

### Supporting custom SQL dialects

This is a work in progress, but we have some notes on [writing a custom SQL
parser](docs/custom_sql_parser.md).

## Design

The core expression parser uses the [Pratt Parser] design, which is a top-down
operator-precedence (TDOP) parser, while the surrounding SQL statement parser is
a traditional, hand-written recursive descent parser. Eli Bendersky has a good
[tutorial on TDOP parsers][tdop-tutorial], if you are interested in learning
more about the technique.

We are a fan of this design pattern over parser generators for the following
reasons:

- Code is simple to write and can be concise and elegant
- Performance is generally better than code generated by parser generators
- Debugging is much easier with hand-written code
- It is far easier to extend and make dialect-specific extensions
  compared to using a parser generator

## Contributing

Contributions are highly encouraged!

Pull requests that add support for or fix a bug in a feature in the SQL
standard, or a feature in a popular RDBMS, like Microsoft SQL Server or
PostgreSQL, will almost certainly be accepted after a brief review. For
particularly large or invasive changes, consider opening an issue first,
especially if you are a first time contributor, so that you can coordinate with
the maintainers. CI will ensure that your code passes `cargo test`,
`cargo fmt`, and `cargo clippy`, so you will likely want to run all three
commands locally before submitting your PR.

If you are unable to submit a patch, feel free to file an issue instead. Please
try to include:

  * some representative examples of the syntax you wish to support or fix;
  * the relevant bits of the [SQL grammar][sql-2016-grammar], if the syntax is
    part of SQL:2016; and
  * links to documentation for the feature for a few of the most popular
    databases that support it.

Please be aware that, while we strive to address bugs and review PRs quickly, we
make no such guarantees for feature requests. If you need support for a feature,
you will likely need to implement it yourself. Our goal as maintainers is to
facilitate the integration of various features from various contributors, but
not to provide the implementations ourselves, as we simply don't have the
resources.

[tdop-tutorial]: https://eli.thegreenplace.net/2010/01/02/top-down-operator-precedence-parsing
[`cargo fmt`]: https://github.com/rust-lang/rustfmt#on-the-stable-toolchain
[current issues]: https://github.com/andygrove/sqlparser-rs/issues
[DataFusion]: https://github.com/apache/arrow/tree/master/rust/datafusion
[LocustDB]: https://github.com/cswinter/LocustDB
[Pratt Parser]: https://tdop.github.io/
[sql-2016-grammar]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html
[sql-standard]: https://en.wikipedia.org/wiki/ISO/IEC_9075
