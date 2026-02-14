// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQL Abstract Syntax Tree (AST) types for table constraints

use crate::ast::{
    display_comma_separated, display_separated, ConstraintCharacteristics,
    ConstraintReferenceMatchKind, Expr, Ident, IndexColumn, IndexOption, IndexType,
    KeyOrIndexDisplay, NullsDistinctOption, ObjectName, ReferentialAction,
};
use crate::tokenizer::Span;
use core::fmt;

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

/// A table-level constraint, specified in a `CREATE TABLE` or an
/// `ALTER TABLE ADD <constraint>` statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableConstraint {
    /// MySQL [definition][1] for `UNIQUE` constraints statements:\
    /// * `[CONSTRAINT [<name>]] UNIQUE <index_type_display> [<index_name>] [index_type] (<columns>) <index_options>`
    ///
    /// where:
    /// * [index_type][2] is `USING {BTREE | HASH}`
    /// * [index_options][3] is `{index_type | COMMENT 'string' | ... %currently unsupported stmts% } ...`
    /// * [index_type_display][4] is `[INDEX | KEY]`
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.3/en/create-table.html
    /// [2]: IndexType
    /// [3]: IndexOption
    /// [4]: KeyOrIndexDisplay
    Unique(UniqueConstraint),
    /// MySQL [definition][1] for `PRIMARY KEY` constraints statements:\
    /// * `[CONSTRAINT [<name>]] PRIMARY KEY [index_name] [index_type] (<columns>) <index_options>`
    ///
    /// Actually the specification have no `[index_name]` but the next query will complete successfully:
    /// ```sql
    /// CREATE TABLE unspec_table (
    ///   xid INT NOT NULL,
    ///   CONSTRAINT p_name PRIMARY KEY index_name USING BTREE (xid)
    /// );
    /// ```
    ///
    /// where:
    /// * [index_type][2] is `USING {BTREE | HASH}`
    /// * [index_options][3] is `{index_type | COMMENT 'string' | ... %currently unsupported stmts% } ...`
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.3/en/create-table.html
    /// [2]: IndexType
    /// [3]: IndexOption
    PrimaryKey(PrimaryKeyConstraint),
    /// A referential integrity constraint (`[ CONSTRAINT <name> ] FOREIGN KEY (<columns>)
    /// REFERENCES <foreign_table> (<referred_columns>)
    /// { [ON DELETE <referential_action>] [ON UPDATE <referential_action>] |
    ///   [ON UPDATE <referential_action>] [ON DELETE <referential_action>]
    /// }`).
    ForeignKey(ForeignKeyConstraint),
    /// `[ CONSTRAINT <name> ] CHECK (<expr>) [[NOT] ENFORCED]`
    Check(CheckConstraint),
    /// MySQLs [index definition][1] for index creation. Not present on ANSI so, for now, the usage
    /// is restricted to MySQL, as no other dialects that support this syntax were found.
    ///
    /// `{INDEX | KEY} [index_name] [index_type] (key_part,...) [index_option]...`
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
    Index(IndexConstraint),
    /// MySQLs [fulltext][1] definition. Since the [`SPATIAL`][2] definition is exactly the same,
    /// and MySQL displays both the same way, it is part of this definition as well.
    ///
    /// Supported syntax:
    ///
    /// ```markdown
    /// {FULLTEXT | SPATIAL} [INDEX | KEY] [index_name] (key_part,...)
    ///
    /// key_part: col_name
    /// ```
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/fulltext-natural-language.html
    /// [2]: https://dev.mysql.com/doc/refman/8.0/en/spatial-types.html
    FulltextOrSpatial(FullTextOrSpatialConstraint),
    /// PostgreSQL [definition][1] for promoting an existing unique index to a
    /// `PRIMARY KEY` or `UNIQUE` constraint:
    ///
    /// `[ CONSTRAINT constraint_name ] { UNIQUE | PRIMARY KEY } USING INDEX index_name
    ///   [ DEFERRABLE | NOT DEFERRABLE ] [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]`
    ///
    /// [1]: https://www.postgresql.org/docs/current/sql-altertable.html
    ConstraintUsingIndex(ConstraintUsingIndex),
}

impl From<UniqueConstraint> for TableConstraint {
    fn from(constraint: UniqueConstraint) -> Self {
        TableConstraint::Unique(constraint)
    }
}

impl From<PrimaryKeyConstraint> for TableConstraint {
    fn from(constraint: PrimaryKeyConstraint) -> Self {
        TableConstraint::PrimaryKey(constraint)
    }
}

impl From<ForeignKeyConstraint> for TableConstraint {
    fn from(constraint: ForeignKeyConstraint) -> Self {
        TableConstraint::ForeignKey(constraint)
    }
}

impl From<CheckConstraint> for TableConstraint {
    fn from(constraint: CheckConstraint) -> Self {
        TableConstraint::Check(constraint)
    }
}

impl From<IndexConstraint> for TableConstraint {
    fn from(constraint: IndexConstraint) -> Self {
        TableConstraint::Index(constraint)
    }
}

impl From<FullTextOrSpatialConstraint> for TableConstraint {
    fn from(constraint: FullTextOrSpatialConstraint) -> Self {
        TableConstraint::FulltextOrSpatial(constraint)
    }
}

impl From<ConstraintUsingIndex> for TableConstraint {
    fn from(constraint: ConstraintUsingIndex) -> Self {
        TableConstraint::ConstraintUsingIndex(constraint)
    }
}

impl fmt::Display for TableConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableConstraint::Unique(constraint) => constraint.fmt(f),
            TableConstraint::PrimaryKey(constraint) => constraint.fmt(f),
            TableConstraint::ForeignKey(constraint) => constraint.fmt(f),
            TableConstraint::Check(constraint) => constraint.fmt(f),
            TableConstraint::Index(constraint) => constraint.fmt(f),
            TableConstraint::FulltextOrSpatial(constraint) => constraint.fmt(f),
            TableConstraint::ConstraintUsingIndex(constraint) => constraint.fmt(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// A `CHECK` constraint (`[ CONSTRAINT <name> ] CHECK (<expr>) [[NOT] ENFORCED]`).
pub struct CheckConstraint {
    /// Optional constraint name.
    pub name: Option<Ident>,
    /// The boolean expression the CHECK constraint enforces.
    pub expr: Box<Expr>,
    /// MySQL-specific `ENFORCED` / `NOT ENFORCED` flag.
    /// <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
    pub enforced: Option<bool>,
}

impl fmt::Display for CheckConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::ast::ddl::display_constraint_name;
        write!(
            f,
            "{}CHECK ({})",
            display_constraint_name(&self.name),
            self.expr
        )?;
        if let Some(b) = self.enforced {
            write!(f, " {}", if b { "ENFORCED" } else { "NOT ENFORCED" })
        } else {
            Ok(())
        }
    }
}

impl crate::ast::Spanned for CheckConstraint {
    fn span(&self) -> Span {
        self.expr
            .span()
            .union_opt(&self.name.as_ref().map(|i| i.span))
    }
}

/// A referential integrity constraint (`[ CONSTRAINT <name> ] FOREIGN KEY (<columns>)
/// REFERENCES <foreign_table> (<referred_columns>) [ MATCH { FULL | PARTIAL | SIMPLE } ]
/// { [ON DELETE <referential_action>] [ON UPDATE <referential_action>] |
///   [ON UPDATE <referential_action>] [ON DELETE <referential_action>]
/// }`).
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ForeignKeyConstraint {
    /// Optional constraint name.
    pub name: Option<Ident>,
    /// MySQL-specific index name associated with the foreign key.
    /// <https://dev.mysql.com/doc/refman/8.4/en/create-table-foreign-keys.html>
    pub index_name: Option<Ident>,
    /// Columns in the local table that participate in the foreign key.
    pub columns: Vec<Ident>,
    /// Referenced foreign table name.
    pub foreign_table: ObjectName,
    /// Columns in the referenced table.
    pub referred_columns: Vec<Ident>,
    /// Action to perform `ON DELETE`.
    pub on_delete: Option<ReferentialAction>,
    /// Action to perform `ON UPDATE`.
    pub on_update: Option<ReferentialAction>,
    /// Optional `MATCH` kind (FULL | PARTIAL | SIMPLE).
    pub match_kind: Option<ConstraintReferenceMatchKind>,
    /// Optional characteristics (e.g., `DEFERRABLE`).
    pub characteristics: Option<ConstraintCharacteristics>,
}

impl fmt::Display for ForeignKeyConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::ast::ddl::{display_constraint_name, display_option_spaced};
        write!(
            f,
            "{}FOREIGN KEY{} ({}) REFERENCES {}",
            display_constraint_name(&self.name),
            display_option_spaced(&self.index_name),
            display_comma_separated(&self.columns),
            self.foreign_table,
        )?;
        if !self.referred_columns.is_empty() {
            write!(f, "({})", display_comma_separated(&self.referred_columns))?;
        }
        if let Some(match_kind) = &self.match_kind {
            write!(f, " {match_kind}")?;
        }
        if let Some(action) = &self.on_delete {
            write!(f, " ON DELETE {action}")?;
        }
        if let Some(action) = &self.on_update {
            write!(f, " ON UPDATE {action}")?;
        }
        if let Some(characteristics) = &self.characteristics {
            write!(f, " {characteristics}")?;
        }
        Ok(())
    }
}

impl crate::ast::Spanned for ForeignKeyConstraint {
    fn span(&self) -> Span {
        fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
            Span::union_iter(iter)
        }

        union_spans(
            self.name
                .iter()
                .map(|i| i.span)
                .chain(self.index_name.iter().map(|i| i.span))
                .chain(self.columns.iter().map(|i| i.span))
                .chain(core::iter::once(self.foreign_table.span()))
                .chain(self.referred_columns.iter().map(|i| i.span))
                .chain(self.on_delete.iter().map(|i| i.span()))
                .chain(self.on_update.iter().map(|i| i.span()))
                .chain(self.characteristics.iter().map(|i| i.span())),
        )
    }
}

/// MySQLs [fulltext][1] definition. Since the [`SPATIAL`][2] definition is exactly the same,
/// and MySQL displays both the same way, it is part of this definition as well.
///
/// Supported syntax:
///
/// ```markdown
/// {FULLTEXT | SPATIAL} [INDEX | KEY] [index_name] (key_part,...)
///
/// key_part: col_name
/// ```
///
/// [1]: https://dev.mysql.com/doc/refman/8.0/en/fulltext-natural-language.html
/// [2]: https://dev.mysql.com/doc/refman/8.0/en/spatial-types.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct FullTextOrSpatialConstraint {
    /// Whether this is a `FULLTEXT` (true) or `SPATIAL` (false) definition.
    pub fulltext: bool,
    /// Whether the type is followed by the keyword `KEY`, `INDEX`, or no keyword at all.
    pub index_type_display: KeyOrIndexDisplay,
    /// Optional index name.
    pub opt_index_name: Option<Ident>,
    /// Referred column identifier list.
    pub columns: Vec<IndexColumn>,
}

impl fmt::Display for FullTextOrSpatialConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.fulltext {
            write!(f, "FULLTEXT")?;
        } else {
            write!(f, "SPATIAL")?;
        }

        write!(f, "{:>}", self.index_type_display)?;

        if let Some(name) = &self.opt_index_name {
            write!(f, " {name}")?;
        }

        write!(f, " ({})", display_comma_separated(&self.columns))?;

        Ok(())
    }
}

impl crate::ast::Spanned for FullTextOrSpatialConstraint {
    fn span(&self) -> Span {
        fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
            Span::union_iter(iter)
        }

        union_spans(
            self.opt_index_name
                .iter()
                .map(|i| i.span)
                .chain(self.columns.iter().map(|i| i.span())),
        )
    }
}

/// MySQLs [index definition][1] for index creation. Not present on ANSI so, for now, the usage
/// is restricted to MySQL, as no other dialects that support this syntax were found.
///
/// `{INDEX | KEY} [index_name] [index_type] (key_part,...) [index_option]...`
///
/// [1]: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IndexConstraint {
    /// Whether this index starts with KEY (true) or INDEX (false), to maintain the same syntax.
    pub display_as_key: bool,
    /// Index name.
    pub name: Option<Ident>,
    /// Optional [index type][1].
    ///
    /// [1]: IndexType
    pub index_type: Option<IndexType>,
    /// Referred column identifier list.
    pub columns: Vec<IndexColumn>,
    /// Optional index options such as `USING`; see [`IndexOption`].
    /// Options applied to the index (e.g., `COMMENT`, `WITH` options).
    pub index_options: Vec<IndexOption>,
}

impl fmt::Display for IndexConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", if self.display_as_key { "KEY" } else { "INDEX" })?;
        if let Some(name) = &self.name {
            write!(f, " {name}")?;
        }
        if let Some(index_type) = &self.index_type {
            write!(f, " USING {index_type}")?;
        }
        write!(f, " ({})", display_comma_separated(&self.columns))?;
        if !self.index_options.is_empty() {
            write!(f, " {}", display_comma_separated(&self.index_options))?;
        }
        Ok(())
    }
}

impl crate::ast::Spanned for IndexConstraint {
    fn span(&self) -> Span {
        fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
            Span::union_iter(iter)
        }

        union_spans(
            self.name
                .iter()
                .map(|i| i.span)
                .chain(self.columns.iter().map(|i| i.span())),
        )
    }
}

/// MySQL [definition][1] for `PRIMARY KEY` constraints statements:
/// * `[CONSTRAINT [<name>]] PRIMARY KEY [index_name] [index_type] (<columns>) <index_options>`
///
/// Actually the specification have no `[index_name]` but the next query will complete successfully:
/// ```sql
/// CREATE TABLE unspec_table (
///   xid INT NOT NULL,
///   CONSTRAINT p_name PRIMARY KEY index_name USING BTREE (xid)
/// );
/// ```
///
/// where:
/// * [index_type][2] is `USING {BTREE | HASH}`
/// * [index_options][3] is `{index_type | COMMENT 'string' | ... %currently unsupported stmts% } ...`
///
/// [1]: https://dev.mysql.com/doc/refman/8.3/en/create-table.html
/// [2]: IndexType
/// [3]: IndexOption
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PrimaryKeyConstraint {
    /// Constraint name.
    ///
    /// Can be not the same as `index_name`
    pub name: Option<Ident>,
    /// Index name
    pub index_name: Option<Ident>,
    /// Optional `USING` of [index type][1] statement before columns.
    ///
    /// [1]: IndexType
    pub index_type: Option<IndexType>,
    /// Identifiers of the columns that form the primary key.
    pub columns: Vec<IndexColumn>,
    /// Optional index options such as `USING`.
    pub index_options: Vec<IndexOption>,
    /// Optional characteristics like `DEFERRABLE`.
    pub characteristics: Option<ConstraintCharacteristics>,
}

impl fmt::Display for PrimaryKeyConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::ast::ddl::{display_constraint_name, display_option, display_option_spaced};
        write!(
            f,
            "{}PRIMARY KEY{}{} ({})",
            display_constraint_name(&self.name),
            display_option_spaced(&self.index_name),
            display_option(" USING ", "", &self.index_type),
            display_comma_separated(&self.columns),
        )?;

        if !self.index_options.is_empty() {
            write!(f, " {}", display_separated(&self.index_options, " "))?;
        }

        write!(f, "{}", display_option_spaced(&self.characteristics))?;
        Ok(())
    }
}

impl crate::ast::Spanned for PrimaryKeyConstraint {
    fn span(&self) -> Span {
        fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
            Span::union_iter(iter)
        }

        union_spans(
            self.name
                .iter()
                .map(|i| i.span)
                .chain(self.index_name.iter().map(|i| i.span))
                .chain(self.columns.iter().map(|i| i.span()))
                .chain(self.characteristics.iter().map(|i| i.span())),
        )
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// Unique constraint definition.
pub struct UniqueConstraint {
    /// Constraint name.
    ///
    /// Can be not the same as `index_name`
    pub name: Option<Ident>,
    /// Index name
    pub index_name: Option<Ident>,
    /// Whether the type is followed by the keyword `KEY`, `INDEX`, or no keyword at all.
    pub index_type_display: KeyOrIndexDisplay,
    /// Optional `USING` of [index type][1] statement before columns.
    ///
    /// [1]: IndexType
    pub index_type: Option<IndexType>,
    /// Identifiers of the columns that are unique.
    pub columns: Vec<IndexColumn>,
    /// Optional index options such as `USING`.
    pub index_options: Vec<IndexOption>,
    /// Optional characteristics like `DEFERRABLE`.
    pub characteristics: Option<ConstraintCharacteristics>,
    /// Optional Postgres nulls handling: `[ NULLS [ NOT ] DISTINCT ]`
    pub nulls_distinct: NullsDistinctOption,
}

impl fmt::Display for UniqueConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::ast::ddl::{display_constraint_name, display_option, display_option_spaced};
        write!(
            f,
            "{}UNIQUE{}{:>}{}{} ({})",
            display_constraint_name(&self.name),
            self.nulls_distinct,
            self.index_type_display,
            display_option_spaced(&self.index_name),
            display_option(" USING ", "", &self.index_type),
            display_comma_separated(&self.columns),
        )?;

        if !self.index_options.is_empty() {
            write!(f, " {}", display_separated(&self.index_options, " "))?;
        }

        write!(f, "{}", display_option_spaced(&self.characteristics))?;
        Ok(())
    }
}

impl crate::ast::Spanned for UniqueConstraint {
    fn span(&self) -> Span {
        fn union_spans<I: Iterator<Item = Span>>(iter: I) -> Span {
            Span::union_iter(iter)
        }

        union_spans(
            self.name
                .iter()
                .map(|i| i.span)
                .chain(self.index_name.iter().map(|i| i.span))
                .chain(self.columns.iter().map(|i| i.span()))
                .chain(self.characteristics.iter().map(|i| i.span())),
        )
    }
}

/// PostgreSQL constraint that promotes an existing unique index to a table constraint.
///
/// `[ CONSTRAINT constraint_name ] { UNIQUE | PRIMARY KEY } USING INDEX index_name
///   [ DEFERRABLE | NOT DEFERRABLE ] [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]`
///
/// See <https://www.postgresql.org/docs/current/sql-altertable.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ConstraintUsingIndex {
    /// Optional constraint name.
    pub name: Option<Ident>,
    /// Whether this is a `PRIMARY KEY` (true) or `UNIQUE` (false) constraint.
    pub is_primary_key: bool,
    /// The name of the existing unique index to promote.
    pub index_name: Ident,
    /// Optional characteristics like `DEFERRABLE`.
    pub characteristics: Option<ConstraintCharacteristics>,
}

impl fmt::Display for ConstraintUsingIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use crate::ast::ddl::{display_constraint_name, display_option_spaced};
        write!(
            f,
            "{}{} USING INDEX {}",
            display_constraint_name(&self.name),
            if self.is_primary_key {
                "PRIMARY KEY"
            } else {
                "UNIQUE"
            },
            self.index_name,
        )?;
        write!(f, "{}", display_option_spaced(&self.characteristics))?;
        Ok(())
    }
}

impl crate::ast::Spanned for ConstraintUsingIndex {
    fn span(&self) -> Span {
        let start = self
            .name
            .as_ref()
            .map(|i| i.span)
            .unwrap_or(self.index_name.span);
        let end = self
            .characteristics
            .as_ref()
            .map(|c| c.span())
            .unwrap_or(self.index_name.span);
        start.union(&end)
    }
}
