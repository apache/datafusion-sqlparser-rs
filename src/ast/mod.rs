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

//! SQL Abstract Syntax Tree (AST) types
#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use helpers::{
    attached_token::AttachedToken,
    stmt_data_loading::{FileStagingCommand, StageLoadSelectItemKind},
};

use core::cmp::Ordering;
use core::ops::Deref;
use core::{
    fmt::{self, Display},
    hash,
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::{
    display_utils::SpaceOrNewline,
    tokenizer::{Span, Token},
};
use crate::{
    display_utils::{Indent, NewLine},
    keywords::Keyword,
};

pub use self::data_type::{
    ArrayElemTypeDef, BinaryLength, CharLengthUnits, CharacterLength, DataType, EnumMember,
    ExactNumberInfo, IntervalFields, StructBracketKind, TimezoneInfo,
};
pub use self::dcl::{
    AlterRoleOperation, CreateRole, ResetConfig, RoleOption, SecondaryRoles, SetConfigValue, Use,
};
pub use self::ddl::{
    Alignment, AlterColumnOperation, AlterConnectorOwner, AlterIndexOperation, AlterOperator,
    AlterOperatorFamily, AlterOperatorFamilyOperation, AlterOperatorOperation,
    AlterPolicyOperation, AlterSchema, AlterSchemaOperation, AlterTable, AlterTableAlgorithm,
    AlterTableLock, AlterTableOperation, AlterTableType, AlterType, AlterTypeAddValue,
    AlterTypeAddValuePosition, AlterTypeOperation, AlterTypeRename, AlterTypeRenameValue,
    ClusteredBy, ColumnDef, ColumnOption, ColumnOptionDef, ColumnOptions, ColumnPolicy,
    ColumnPolicyProperty, ConstraintCharacteristics, CreateConnector, CreateDomain,
    CreateExtension, CreateFunction, CreateIndex, CreateOperator, CreateOperatorClass,
    CreateOperatorFamily, CreateTable, CreateTrigger, CreateView, Deduplicate, DeferrableInitial,
    DropBehavior, DropExtension, DropFunction, DropOperator, DropOperatorClass, DropOperatorFamily,
    DropOperatorSignature, DropTrigger, GeneratedAs, GeneratedExpressionMode, IdentityParameters,
    IdentityProperty, IdentityPropertyFormatKind, IdentityPropertyKind, IdentityPropertyOrder,
    IndexColumn, IndexOption, IndexType, KeyOrIndexDisplay, Msck, NullsDistinctOption,
    OperatorArgTypes, OperatorClassItem, OperatorFamilyDropItem, OperatorFamilyItem,
    OperatorOption, OperatorPurpose, Owner, Partition, ProcedureParam, ReferentialAction,
    RenameTableNameKind, ReplicaIdentity, TagsColumnOption, TriggerObjectKind, Truncate,
    UserDefinedTypeCompositeAttributeDef, UserDefinedTypeInternalLength,
    UserDefinedTypeRangeOption, UserDefinedTypeRepresentation, UserDefinedTypeSqlDefinitionOption,
    UserDefinedTypeStorage, ViewColumnDef,
};
pub use self::dml::{
    Delete, Insert, Merge, MergeAction, MergeClause, MergeClauseKind, MergeInsertExpr,
    MergeInsertKind, MergeUpdateExpr, OutputClause, Update,
};
pub use self::operator::{BinaryOperator, UnaryOperator};
pub use self::query::{
    AfterMatchSkip, ConnectBy, Cte, CteAsMaterialized, Distinct, EmptyMatchesMode,
    ExceptSelectItem, ExcludeSelectItem, ExprWithAlias, ExprWithAliasAndOrderBy, Fetch, ForClause,
    ForJson, ForXml, FormatClause, GroupByExpr, GroupByWithModifier, IdentWithAlias,
    IlikeSelectItem, InputFormatClause, Interpolate, InterpolateExpr, Join, JoinConstraint,
    JoinOperator, JsonTableColumn, JsonTableColumnErrorHandling, JsonTableNamedColumn,
    JsonTableNestedColumn, LateralView, LimitClause, LockClause, LockType, MatchRecognizePattern,
    MatchRecognizeSymbol, Measure, NamedWindowDefinition, NamedWindowExpr, NonBlock, Offset,
    OffsetRows, OpenJsonTableColumn, OrderBy, OrderByExpr, OrderByKind, OrderByOptions,
    PipeOperator, PivotValueSource, ProjectionSelect, Query, RenameSelectItem,
    RepetitionQuantifier, ReplaceSelectElement, ReplaceSelectItem, RowsPerMatch, Select,
    SelectFlavor, SelectInto, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, SetOperator,
    SetQuantifier, Setting, SymbolDefinition, Table, TableAlias, TableAliasColumnDef, TableFactor,
    TableFunctionArgs, TableIndexHintForClause, TableIndexHintType, TableIndexHints,
    TableIndexType, TableSample, TableSampleBucket, TableSampleKind, TableSampleMethod,
    TableSampleModifier, TableSampleQuantity, TableSampleSeed, TableSampleSeedModifier,
    TableSampleUnit, TableVersion, TableWithJoins, Top, TopQuantity, UpdateTableFromKind,
    ValueTableMode, Values, WildcardAdditionalOptions, With, WithFill, XmlNamespaceDefinition,
    XmlPassingArgument, XmlPassingClause, XmlTableColumn, XmlTableColumnOption,
};

pub use self::trigger::{
    TriggerEvent, TriggerExecBody, TriggerExecBodyType, TriggerObject, TriggerPeriod,
    TriggerReferencing, TriggerReferencingType,
};

pub use self::value::{
    escape_double_quote_string, escape_quoted_string, DateTimeField, DollarQuotedString,
    NormalizationForm, QuoteDelimitedString, TrimWhereField, Value, ValueWithSpan,
};

use crate::ast::helpers::key_value_options::KeyValueOptions;
use crate::ast::helpers::stmt_data_loading::StageParamsObject;

#[cfg(feature = "visitor")]
pub use visitor::*;

pub use self::data_type::GeometricTypeKind;

mod data_type;
mod dcl;
mod ddl;
mod dml;
pub mod helpers;
pub mod table_constraints;
pub use table_constraints::{
    CheckConstraint, ForeignKeyConstraint, FullTextOrSpatialConstraint, IndexConstraint,
    PrimaryKeyConstraint, TableConstraint, UniqueConstraint,
};
mod operator;
mod query;
mod spans;
pub use spans::Spanned;

pub mod comments;
mod trigger;
mod value;

#[cfg(feature = "visitor")]
mod visitor;

pub struct DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<T> fmt::Display for DisplaySeparated<'_, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut delim = "";
        for t in self.slice {
            f.write_str(delim)?;
            delim = self.sep;
            t.fmt(f)?;
        }
        Ok(())
    }
}

pub(crate) fn display_separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep }
}

pub(crate) fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep: ", " }
}

/// Writes the given statements to the formatter, each ending with
/// a semicolon and space separated.
fn format_statement_list(f: &mut fmt::Formatter, statements: &[Statement]) -> fmt::Result {
    write!(f, "{}", display_separated(statements, "; "))?;
    // We manually insert semicolon for the last statement,
    // since display_separated doesn't handle that case.
    write!(f, ";")
}

/// An identifier, decomposed into its value or character data and the quote style.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Ident {
    /// The value of the identifier without quotes.
    pub value: String,
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub quote_style: Option<char>,
    /// The span of the identifier in the original SQL string.
    pub span: Span,
}

impl PartialEq for Ident {
    fn eq(&self, other: &Self) -> bool {
        let Ident {
            value,
            quote_style,
            // exhaustiveness check; we ignore spans in comparisons
            span: _,
        } = self;

        value == &other.value && quote_style == &other.quote_style
    }
}

impl core::hash::Hash for Ident {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        let Ident {
            value,
            quote_style,
            // exhaustiveness check; we ignore spans in hashes
            span: _,
        } = self;

        value.hash(state);
        quote_style.hash(state);
    }
}

impl Eq for Ident {}

impl PartialOrd for Ident {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ident {
    fn cmp(&self, other: &Self) -> Ordering {
        let Ident {
            value,
            quote_style,
            // exhaustiveness check; we ignore spans in ordering
            span: _,
        } = self;

        let Ident {
            value: other_value,
            quote_style: other_quote_style,
            // exhaustiveness check; we ignore spans in ordering
            span: _,
        } = other;

        // First compare by value, then by quote_style
        value
            .cmp(other_value)
            .then_with(|| quote_style.cmp(other_quote_style))
    }
}

impl Ident {
    /// Create a new identifier with the given value and no quotes and an empty span.
    pub fn new<S>(value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: None,
            span: Span::empty(),
        }
    }

    /// Create a new quoted identifier with the given quote and value. This function
    /// panics if the given quote is not a valid quote character.
    pub fn with_quote<S>(quote: char, value: S) -> Self
    where
        S: Into<String>,
    {
        assert!(quote == '\'' || quote == '"' || quote == '`' || quote == '[');
        Ident {
            value: value.into(),
            quote_style: Some(quote),
            span: Span::empty(),
        }
    }

    pub fn with_span<S>(span: Span, value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: None,
            span,
        }
    }

    pub fn with_quote_and_span<S>(quote: char, span: Span, value: S) -> Self
    where
        S: Into<String>,
    {
        assert!(quote == '\'' || quote == '"' || quote == '`' || quote == '[');
        Ident {
            value: value.into(),
            quote_style: Some(quote),
            span,
        }
    }
}

impl From<&str> for Ident {
    fn from(value: &str) -> Self {
        Ident {
            value: value.to_string(),
            quote_style: None,
            span: Span::empty(),
        }
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.quote_style {
            Some(q) if q == '"' || q == '\'' || q == '`' => {
                let escaped = value::escape_quoted_string(&self.value, q);
                write!(f, "{q}{escaped}{q}")
            }
            Some('[') => write!(f, "[{}]", self.value),
            None => f.write_str(&self.value),
            _ => panic!("unexpected quote style"),
        }
    }
}

/// A name of a table, view, custom type, etc., possibly multi-part, i.e. db.schema.obj
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ObjectName(pub Vec<ObjectNamePart>);

impl From<Vec<Ident>> for ObjectName {
    fn from(idents: Vec<Ident>) -> Self {
        ObjectName(idents.into_iter().map(ObjectNamePart::Identifier).collect())
    }
}

impl From<Ident> for ObjectName {
    fn from(ident: Ident) -> Self {
        ObjectName(vec![ObjectNamePart::Identifier(ident)])
    }
}

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", display_separated(&self.0, "."))
    }
}

/// A single part of an ObjectName
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ObjectNamePart {
    Identifier(Ident),
    Function(ObjectNamePartFunction),
}

impl ObjectNamePart {
    pub fn as_ident(&self) -> Option<&Ident> {
        match self {
            ObjectNamePart::Identifier(ident) => Some(ident),
            ObjectNamePart::Function(_) => None,
        }
    }
}

impl fmt::Display for ObjectNamePart {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ObjectNamePart::Identifier(ident) => write!(f, "{ident}"),
            ObjectNamePart::Function(func) => write!(f, "{func}"),
        }
    }
}

/// An object name part that consists of a function that dynamically
/// constructs identifiers.
///
/// - [Snowflake](https://docs.snowflake.com/en/sql-reference/identifier-literal)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ObjectNamePartFunction {
    pub name: Ident,
    pub args: Vec<FunctionArg>,
}

impl fmt::Display for ObjectNamePartFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        write!(f, "{})", display_comma_separated(&self.args))
    }
}

/// Represents an Array Expression, either
/// `ARRAY[..]`, or `[..]`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Array {
    /// The list of expressions between brackets
    pub elem: Vec<Expr>,

    /// `true` for  `ARRAY[..]`, `false` for `[..]`
    pub named: bool,
}

impl fmt::Display for Array {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}[{}]",
            if self.named { "ARRAY" } else { "" },
            display_comma_separated(&self.elem)
        )
    }
}

/// Represents an INTERVAL expression, roughly in the following format:
/// `INTERVAL '<value>' [ <leading_field> [ (<leading_precision>) ] ]
/// [ TO <last_field> [ (<fractional_seconds_precision>) ] ]`,
/// e.g. `INTERVAL '123:45.67' MINUTE(3) TO SECOND(2)`.
///
/// The parser does not validate the `<value>`, nor does it ensure
/// that the `<leading_field>` units >= the units in `<last_field>`,
/// so the user will have to reject intervals like `HOUR TO YEAR`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Interval {
    pub value: Box<Expr>,
    pub leading_field: Option<DateTimeField>,
    pub leading_precision: Option<u64>,
    pub last_field: Option<DateTimeField>,
    /// The seconds precision can be specified in SQL source as
    /// `INTERVAL '__' SECOND(_, x)` (in which case the `leading_field`
    /// will be `Second` and the `last_field` will be `None`),
    /// or as `__ TO SECOND(x)`.
    pub fractional_seconds_precision: Option<u64>,
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let value = self.value.as_ref();
        match (
            &self.leading_field,
            self.leading_precision,
            self.fractional_seconds_precision,
        ) {
            (
                Some(DateTimeField::Second),
                Some(leading_precision),
                Some(fractional_seconds_precision),
            ) => {
                // When the leading field is SECOND, the parser guarantees that
                // the last field is None.
                assert!(self.last_field.is_none());
                write!(
                    f,
                    "INTERVAL {value} SECOND ({leading_precision}, {fractional_seconds_precision})"
                )
            }
            _ => {
                write!(f, "INTERVAL {value}")?;
                if let Some(leading_field) = &self.leading_field {
                    write!(f, " {leading_field}")?;
                }
                if let Some(leading_precision) = self.leading_precision {
                    write!(f, " ({leading_precision})")?;
                }
                if let Some(last_field) = &self.last_field {
                    write!(f, " TO {last_field}")?;
                }
                if let Some(fractional_seconds_precision) = self.fractional_seconds_precision {
                    write!(f, " ({fractional_seconds_precision})")?;
                }
                Ok(())
            }
        }
    }
}

/// A field definition within a struct
///
/// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct StructField {
    pub field_name: Option<Ident>,
    pub field_type: DataType,
    /// Struct field options.
    /// See [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#column_name_and_column_schema)
    pub options: Option<Vec<SqlOption>>,
}

impl fmt::Display for StructField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.field_name {
            write!(f, "{name} {}", self.field_type)?;
        } else {
            write!(f, "{}", self.field_type)?;
        }
        if let Some(options) = &self.options {
            write!(f, " OPTIONS({})", display_separated(options, ", "))
        } else {
            Ok(())
        }
    }
}

/// A field definition within a union
///
/// [DuckDB]: https://duckdb.org/docs/sql/data_types/union.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct UnionField {
    pub field_name: Ident,
    pub field_type: DataType,
}

impl fmt::Display for UnionField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.field_name, self.field_type)
    }
}

/// A dictionary field within a dictionary.
///
/// [DuckDB]: https://duckdb.org/docs/sql/data_types/struct#creating-structs
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DictionaryField {
    pub key: Ident,
    pub value: Box<Expr>,
}

impl fmt::Display for DictionaryField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.key, self.value)
    }
}

/// Represents a Map expression.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Map {
    pub entries: Vec<MapEntry>,
}

impl Display for Map {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAP {{{}}}", display_comma_separated(&self.entries))
    }
}

/// A map field within a map.
///
/// [DuckDB]: https://duckdb.org/docs/sql/data_types/map.html#creating-maps
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MapEntry {
    pub key: Box<Expr>,
    pub value: Box<Expr>,
}

impl fmt::Display for MapEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.key, self.value)
    }
}

/// Options for `CAST` / `TRY_CAST`
/// BigQuery: <https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#formatting_syntax>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CastFormat {
    Value(Value),
    ValueAtTimeZone(Value, Value),
}

/// An element of a JSON path.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JsonPathElem {
    /// Accesses an object field using dot notation, e.g. `obj:foo.bar.baz`.
    ///
    /// See <https://docs.snowflake.com/en/user-guide/querying-semistructured#dot-notation>.
    Dot { key: String, quoted: bool },
    /// Accesses an object field or array element using bracket notation,
    /// e.g. `obj['foo']`.
    ///
    /// See <https://docs.snowflake.com/en/user-guide/querying-semistructured#bracket-notation>.
    Bracket { key: Expr },
}

/// A JSON path.
///
/// See <https://docs.snowflake.com/en/user-guide/querying-semistructured>.
/// See <https://docs.databricks.com/en/sql/language-manual/sql-ref-json-path-expression.html>.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct JsonPath {
    pub path: Vec<JsonPathElem>,
}

impl fmt::Display for JsonPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, elem) in self.path.iter().enumerate() {
            match elem {
                JsonPathElem::Dot { key, quoted } => {
                    if i == 0 {
                        write!(f, ":")?;
                    } else {
                        write!(f, ".")?;
                    }

                    if *quoted {
                        write!(f, "\"{}\"", escape_double_quote_string(key))?;
                    } else {
                        write!(f, "{key}")?;
                    }
                }
                JsonPathElem::Bracket { key } => {
                    write!(f, "[{key}]")?;
                }
            }
        }
        Ok(())
    }
}

/// The syntax used for in a cast expression.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CastKind {
    /// The standard SQL cast syntax, e.g. `CAST(<expr> as <datatype>)`
    Cast,
    /// A cast that returns `NULL` on failure, e.g. `TRY_CAST(<expr> as <datatype>)`.
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/functions/try_cast>.
    /// See <https://learn.microsoft.com/en-us/sql/t-sql/functions/try-cast-transact-sql>.
    TryCast,
    /// A cast that returns `NULL` on failure, bigQuery-specific ,  e.g. `SAFE_CAST(<expr> as <datatype>)`.
    ///
    /// See <https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#safe_casting>.
    SafeCast,
    /// `<expr> :: <datatype>`
    DoubleColon,
}

/// `MATCH` type for constraint references
///
/// See: <https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-PARMS-REFERENCES>
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ConstraintReferenceMatchKind {
    /// `MATCH FULL`
    Full,
    /// `MATCH PARTIAL`
    Partial,
    /// `MATCH SIMPLE`
    Simple,
}

impl fmt::Display for ConstraintReferenceMatchKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Full => write!(f, "MATCH FULL"),
            Self::Partial => write!(f, "MATCH PARTIAL"),
            Self::Simple => write!(f, "MATCH SIMPLE"),
        }
    }
}

/// `EXTRACT` syntax variants.
///
/// In Snowflake dialect, the `EXTRACT` expression can support either the `from` syntax
/// or the comma syntax.
///
/// See <https://docs.snowflake.com/en/sql-reference/functions/extract>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ExtractSyntax {
    /// `EXTRACT( <date_or_time_part> FROM <date_or_time_expr> )`
    From,
    /// `EXTRACT( <date_or_time_part> , <date_or_timestamp_expr> )`
    Comma,
}

/// The syntax used in a CEIL or FLOOR expression.
///
/// The `CEIL/FLOOR(<datetime value expression> TO <time unit>)` is an Amazon Kinesis Data Analytics extension.
/// See <https://docs.aws.amazon.com/kinesisanalytics/latest/sqlref/sql-reference-ceil.html> for
/// details.
///
/// Other dialects either support `CEIL/FLOOR( <expr> [, <scale>])` format or just
/// `CEIL/FLOOR(<expr>)`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CeilFloorKind {
    /// `CEIL( <expr> TO <DateTimeField>)`
    DateTimeField(DateTimeField),
    /// `CEIL( <expr> [, <scale>])`
    Scale(Value),
}

/// A WHEN clause in a CASE expression containing both
/// the condition and its corresponding result
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CaseWhen {
    pub condition: Expr,
    pub result: Expr,
}

impl fmt::Display for CaseWhen {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("WHEN ")?;
        self.condition.fmt(f)?;
        f.write_str(" THEN")?;
        SpaceOrNewline.fmt(f)?;
        Indent(&self.result).fmt(f)?;
        Ok(())
    }
}

/// An SQL expression of any type.
///
/// # Semantics / Type Checking
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string). The caller is responsible for detecting and
/// validating types as necessary (for example  `WHERE 1` vs `SELECT 1=1`)
/// See the [README.md] for more details.
///
/// [README.md]: https://github.com/apache/datafusion-sqlparser-rs/blob/main/README.md#syntax-vs-semantics
///
/// # Equality and Hashing Does not Include Source Locations
///
/// The `Expr` type implements `PartialEq` and `Eq` based on the semantic value
/// of the expression (not bitwise comparison). This means that `Expr` instances
/// that are semantically equivalent but have different spans (locations in the
/// source tree) will compare as equal.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(
    feature = "visitor",
    derive(Visit, VisitMut),
    visit(with = "visit_expr")
)]
pub enum Expr {
    /// Identifier e.g. table name or column name
    Identifier(Ident),
    /// Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
    CompoundIdentifier(Vec<Ident>),
    /// Multi-part expression access.
    ///
    /// This structure represents an access chain in structured / nested types
    /// such as maps, arrays, and lists:
    /// - Array
    ///     - A 1-dim array `a[1]` will be represented like:
    ///       `CompoundFieldAccess(Ident('a'), vec![Subscript(1)]`
    ///     - A 2-dim array `a[1][2]` will be represented like:
    ///       `CompoundFieldAccess(Ident('a'), vec![Subscript(1), Subscript(2)]`
    /// - Map or Struct (Bracket-style)
    ///     - A map `a['field1']` will be represented like:
    ///       `CompoundFieldAccess(Ident('a'), vec![Subscript('field')]`
    ///     - A 2-dim map `a['field1']['field2']` will be represented like:
    ///       `CompoundFieldAccess(Ident('a'), vec![Subscript('field1'), Subscript('field2')]`
    /// - Struct (Dot-style) (only effect when the chain contains both subscript and expr)
    ///     - A struct access `a[field1].field2` will be represented like:
    ///       `CompoundFieldAccess(Ident('a'), vec![Subscript('field1'), Ident('field2')]`
    /// - If a struct access likes `a.field1.field2`, it will be represented by CompoundIdentifier([a, field1, field2])
    CompoundFieldAccess {
        root: Box<Expr>,
        access_chain: Vec<AccessExpr>,
    },
    /// Access data nested in a value containing semi-structured data, such as
    /// the `VARIANT` type on Snowflake. for example `src:customer[0].name`.
    ///
    /// See <https://docs.snowflake.com/en/user-guide/querying-semistructured>.
    /// See <https://docs.databricks.com/en/sql/language-manual/functions/colonsign.html>.
    JsonAccess {
        /// The value being queried.
        value: Box<Expr>,
        /// The path to the data to extract.
        path: JsonPath,
    },
    /// `IS FALSE` operator
    IsFalse(Box<Expr>),
    /// `IS NOT FALSE` operator
    IsNotFalse(Box<Expr>),
    /// `IS TRUE` operator
    IsTrue(Box<Expr>),
    /// `IS NOT TRUE` operator
    IsNotTrue(Box<Expr>),
    /// `IS NULL` operator
    IsNull(Box<Expr>),
    /// `IS NOT NULL` operator
    IsNotNull(Box<Expr>),
    /// `IS UNKNOWN` operator
    IsUnknown(Box<Expr>),
    /// `IS NOT UNKNOWN` operator
    IsNotUnknown(Box<Expr>),
    /// `IS DISTINCT FROM` operator
    IsDistinctFrom(Box<Expr>, Box<Expr>),
    /// `IS NOT DISTINCT FROM` operator
    IsNotDistinctFrom(Box<Expr>, Box<Expr>),
    /// `<expr> IS [ NOT ] [ form ] NORMALIZED`
    IsNormalized {
        expr: Box<Expr>,
        form: Option<NormalizationForm>,
        negated: bool,
    },
    /// `[ NOT ] IN (val1, val2, ...)`
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// `[ NOT ] IN UNNEST(array_expression)`
    InUnnest {
        expr: Box<Expr>,
        array_expr: Box<Expr>,
        negated: bool,
    },
    /// `<expr> [ NOT ] BETWEEN <low> AND <high>`
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// Binary operation e.g. `1 + 1` or `foo > bar`
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// `[NOT] LIKE <pattern> [ESCAPE <escape_character>]`
    Like {
        negated: bool,
        // Snowflake supports the ANY keyword to match against a list of patterns
        // https://docs.snowflake.com/en/sql-reference/functions/like_any
        any: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<Value>,
    },
    /// `ILIKE` (case-insensitive `LIKE`)
    ILike {
        negated: bool,
        // Snowflake supports the ANY keyword to match against a list of patterns
        // https://docs.snowflake.com/en/sql-reference/functions/like_any
        any: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<Value>,
    },
    /// SIMILAR TO regex
    SimilarTo {
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<Value>,
    },
    /// MySQL: RLIKE regex or REGEXP regex
    RLike {
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        // true for REGEXP, false for RLIKE (no difference in semantics)
        regexp: bool,
    },
    /// `ANY` operation e.g. `foo > ANY(bar)`, comparison operator is one of `[=, >, <, =>, =<, !=]`
    /// <https://docs.snowflake.com/en/sql-reference/operators-subquery#all-any>
    AnyOp {
        left: Box<Expr>,
        compare_op: BinaryOperator,
        right: Box<Expr>,
        // ANY and SOME are synonymous: https://docs.cloudera.com/cdw-runtime/cloud/using-hiveql/topics/hive_comparison_predicates.html
        is_some: bool,
    },
    /// `ALL` operation e.g. `foo > ALL(bar)`, comparison operator is one of `[=, >, <, =>, =<, !=]`
    /// <https://docs.snowflake.com/en/sql-reference/operators-subquery#all-any>
    AllOp {
        left: Box<Expr>,
        compare_op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operation e.g. `NOT foo`
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    /// CONVERT a value to a different data type or character encoding. e.g. `CONVERT(foo USING utf8mb4)`
    Convert {
        /// CONVERT (false) or TRY_CONVERT (true)
        /// <https://learn.microsoft.com/en-us/sql/t-sql/functions/try-convert-transact-sql?view=sql-server-ver16>
        is_try: bool,
        /// The expression to convert
        expr: Box<Expr>,
        /// The target data type
        data_type: Option<DataType>,
        /// The target character encoding
        charset: Option<ObjectName>,
        /// whether the target comes before the expr (MSSQL syntax)
        target_before_value: bool,
        /// How to translate the expression.
        ///
        /// [MSSQL]: https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16#style
        styles: Vec<Expr>,
    },
    /// `CAST` an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    Cast {
        kind: CastKind,
        expr: Box<Expr>,
        data_type: DataType,
        /// Optional CAST(string_expression AS type FORMAT format_string_expression) as used by [BigQuery]
        ///
        /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#formatting_syntax
        format: Option<CastFormat>,
    },
    /// AT a timestamp to a different timezone e.g. `FROM_UNIXTIME(0) AT TIME ZONE 'UTC-06:00'`
    AtTimeZone {
        timestamp: Box<Expr>,
        time_zone: Box<Expr>,
    },
    /// Extract a field from a timestamp e.g. `EXTRACT(MONTH FROM foo)`
    /// Or `EXTRACT(MONTH, foo)`
    ///
    /// Syntax:
    /// ```sql
    /// EXTRACT(DateTimeField FROM <expr>) | EXTRACT(DateTimeField, <expr>)
    /// ```
    Extract {
        field: DateTimeField,
        syntax: ExtractSyntax,
        expr: Box<Expr>,
    },
    /// ```sql
    /// CEIL(<expr> [TO DateTimeField])
    /// ```
    /// ```sql
    /// CEIL( <input_expr> [, <scale_expr> ] )
    /// ```
    Ceil {
        expr: Box<Expr>,
        field: CeilFloorKind,
    },
    /// ```sql
    /// FLOOR(<expr> [TO DateTimeField])
    /// ```
    /// ```sql
    /// FLOOR( <input_expr> [, <scale_expr> ] )
    ///
    Floor {
        expr: Box<Expr>,
        field: CeilFloorKind,
    },
    /// ```sql
    /// POSITION(<expr> in <expr>)
    /// ```
    Position {
        expr: Box<Expr>,
        r#in: Box<Expr>,
    },
    /// ```sql
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    /// ```
    /// or
    /// ```sql
    /// SUBSTRING(<expr>, <expr>, <expr>)
    /// ```
    Substring {
        expr: Box<Expr>,
        substring_from: Option<Box<Expr>>,
        substring_for: Option<Box<Expr>>,

        /// false if the expression is represented using the `SUBSTRING(expr [FROM start] [FOR len])` syntax
        /// true if the expression is represented using the `SUBSTRING(expr, start, len)` syntax
        /// This flag is used for formatting.
        special: bool,

        /// true if the expression is represented using the `SUBSTR` shorthand
        /// This flag is used for formatting.
        shorthand: bool,
    },
    /// ```sql
    /// TRIM([BOTH | LEADING | TRAILING] [<expr> FROM] <expr>)
    /// TRIM(<expr>)
    /// TRIM(<expr>, [, characters]) -- only Snowflake or Bigquery
    /// ```
    Trim {
        expr: Box<Expr>,
        // ([BOTH | LEADING | TRAILING]
        trim_where: Option<TrimWhereField>,
        trim_what: Option<Box<Expr>>,
        trim_characters: Option<Vec<Expr>>,
    },
    /// ```sql
    /// OVERLAY(<expr> PLACING <expr> FROM <expr>[ FOR <expr> ]
    /// ```
    Overlay {
        expr: Box<Expr>,
        overlay_what: Box<Expr>,
        overlay_from: Box<Expr>,
        overlay_for: Option<Box<Expr>>,
    },
    /// `expr COLLATE collation`
    Collate {
        expr: Box<Expr>,
        collation: ObjectName,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// A literal value, such as string, number, date or NULL
    Value(ValueWithSpan),
    /// Prefixed expression, e.g. introducer strings, projection prefix
    /// <https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html>
    /// <https://docs.snowflake.com/en/sql-reference/constructs/connect-by>
    Prefixed {
        prefix: Ident,
        /// The value of the constant.
        /// Hint: you can unwrap the string value using `value.into_string()`.
        value: Box<Expr>,
    },
    /// A constant of form `<data_type> 'value'`.
    /// This can represent ANSI SQL `DATE`, `TIME`, and `TIMESTAMP` literals (such as `DATE '2020-01-01'`),
    /// as well as constants of other types (a non-standard PostgreSQL extension).
    TypedString(TypedString),
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function),
    /// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
    ///
    /// Note we only recognize a complete single expression as `<condition>`,
    /// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
    /// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
    Case {
        case_token: AttachedToken,
        end_token: AttachedToken,
        operand: Option<Box<Expr>>,
        conditions: Vec<CaseWhen>,
        else_result: Option<Box<Expr>>,
    },
    /// An exists expression `[ NOT ] EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE [ NOT ] EXISTS (SELECT ...)`.
    Exists {
        subquery: Box<Query>,
        negated: bool,
    },
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query>),
    /// The `GROUPING SETS` expr.
    GroupingSets(Vec<Vec<Expr>>),
    /// The `CUBE` expr.
    Cube(Vec<Vec<Expr>>),
    /// The `ROLLUP` expr.
    Rollup(Vec<Vec<Expr>>),
    /// ROW / TUPLE a single value, such as `SELECT (1, 2)`
    Tuple(Vec<Expr>),
    /// `Struct` literal expression
    /// Syntax:
    /// ```sql
    /// STRUCT<[field_name] field_type, ...>( expr1 [, ... ])
    ///
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type)
    /// [Databricks](https://docs.databricks.com/en/sql/language-manual/functions/struct.html)
    /// ```
    Struct {
        /// Struct values.
        values: Vec<Expr>,
        /// Struct field definitions.
        fields: Vec<StructField>,
    },
    /// `BigQuery` specific: An named expression in a typeless struct [1]
    ///
    /// Syntax
    /// ```sql
    /// 1 AS A
    /// ```
    /// [1]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
    Named {
        expr: Box<Expr>,
        name: Ident,
    },
    /// `DuckDB` specific `Struct` literal expression [1]
    ///
    /// Syntax:
    /// ```sql
    /// syntax: {'field_name': expr1[, ... ]}
    /// ```
    /// [1]: https://duckdb.org/docs/sql/data_types/struct#creating-structs
    Dictionary(Vec<DictionaryField>),
    /// `DuckDB` specific `Map` literal expression [1]
    ///
    /// Syntax:
    /// ```sql
    /// syntax: Map {key1: value1[, ... ]}
    /// ```
    /// [1]: https://duckdb.org/docs/sql/data_types/map#creating-maps
    Map(Map),
    /// An array expression e.g. `ARRAY[1, 2]`
    Array(Array),
    /// An interval expression e.g. `INTERVAL '1' YEAR`
    Interval(Interval),
    /// `MySQL` specific text search function [(1)].
    ///
    /// Syntax:
    /// ```sql
    /// MATCH (<col>, <col>, ...) AGAINST (<expr> [<search modifier>])
    ///
    /// <col> = CompoundIdentifier
    /// <expr> = String literal
    /// ```
    /// [(1)]: https://dev.mysql.com/doc/refman/8.0/en/fulltext-search.html#function_match
    MatchAgainst {
        /// `(<col>, <col>, ...)`.
        columns: Vec<ObjectName>,
        /// `<expr>`.
        match_value: Value,
        /// `<search modifier>`
        opt_search_modifier: Option<SearchModifier>,
    },
    Wildcard(AttachedToken),
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    /// (Same caveats apply to `QualifiedWildcard` as to `Wildcard`.)
    QualifiedWildcard(ObjectName, AttachedToken),
    /// Some dialects support an older syntax for outer joins where columns are
    /// marked with the `(+)` operator in the WHERE clause, for example:
    ///
    /// ```sql
    /// SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2 (+)
    /// ```
    ///
    /// which is equivalent to
    ///
    /// ```sql
    /// SELECT t1.c1, t2.c2 FROM t1 LEFT OUTER JOIN t2 ON t1.c1 = t2.c2
    /// ```
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/where#joins-in-the-where-clause>.
    OuterJoin(Box<Expr>),
    /// A reference to the prior level in a CONNECT BY clause.
    Prior(Box<Expr>),
    /// A lambda function.
    ///
    /// Syntax:
    /// ```plaintext
    /// param -> expr | (param1, ...) -> expr
    /// ```
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/functions#higher-order-functions---operator-and-lambdaparams-expr-function)
    /// [Databricks](https://docs.databricks.com/en/sql/language-manual/sql-ref-lambda-functions.html)
    /// [DuckDB](https://duckdb.org/docs/stable/sql/functions/lambda)
    Lambda(LambdaFunction),
    /// Checks membership of a value in a JSON array
    MemberOf(MemberOf),
}

impl Expr {
    /// Creates a new [`Expr::Value`]
    pub fn value(value: impl Into<ValueWithSpan>) -> Self {
        Expr::Value(value.into())
    }
}

/// The contents inside the `[` and `]` in a subscript expression.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Subscript {
    /// Accesses the element of the array at the given index.
    Index { index: Expr },

    /// Accesses a slice of an array on PostgreSQL, e.g.
    ///
    /// ```plaintext
    /// => select (array[1,2,3,4,5,6])[2:5];
    /// -----------
    /// {2,3,4,5}
    /// ```
    ///
    /// The lower and/or upper bound can be omitted to slice from the start or
    /// end of the array respectively.
    ///
    /// See <https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING>.
    ///
    /// Also supports an optional "stride" as the last element (this is not
    /// supported by postgres), e.g.
    ///
    /// ```plaintext
    /// => select (array[1,2,3,4,5,6])[1:6:2];
    /// -----------
    /// {1,3,5}
    /// ```
    Slice {
        lower_bound: Option<Expr>,
        upper_bound: Option<Expr>,
        stride: Option<Expr>,
    },
}

impl fmt::Display for Subscript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Subscript::Index { index } => write!(f, "{index}"),
            Subscript::Slice {
                lower_bound,
                upper_bound,
                stride,
            } => {
                if let Some(lower) = lower_bound {
                    write!(f, "{lower}")?;
                }
                write!(f, ":")?;
                if let Some(upper) = upper_bound {
                    write!(f, "{upper}")?;
                }
                if let Some(stride) = stride {
                    write!(f, ":")?;
                    write!(f, "{stride}")?;
                }
                Ok(())
            }
        }
    }
}

/// An element of a [`Expr::CompoundFieldAccess`].
/// It can be an expression or a subscript.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AccessExpr {
    /// Accesses a field using dot notation, e.g. `foo.bar.baz`.
    Dot(Expr),
    /// Accesses a field or array element using bracket notation, e.g. `foo['bar']`.
    Subscript(Subscript),
}

impl fmt::Display for AccessExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccessExpr::Dot(expr) => write!(f, ".{expr}"),
            AccessExpr::Subscript(subscript) => write!(f, "[{subscript}]"),
        }
    }
}

/// A lambda function.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct LambdaFunction {
    /// The parameters to the lambda function.
    pub params: OneOrManyWithParens<Ident>,
    /// The body of the lambda function.
    pub body: Box<Expr>,
    /// The syntax style used to write the lambda function.
    pub syntax: LambdaSyntax,
}

impl fmt::Display for LambdaFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.syntax {
            LambdaSyntax::Arrow => write!(f, "{} -> {}", self.params, self.body),
            LambdaSyntax::LambdaKeyword => {
                // For lambda keyword syntax, display params without parentheses
                // e.g., `lambda x, y : expr` not `lambda (x, y) : expr`
                write!(f, "lambda ")?;
                match &self.params {
                    OneOrManyWithParens::One(p) => write!(f, "{p}")?,
                    OneOrManyWithParens::Many(ps) => write!(f, "{}", display_comma_separated(ps))?,
                };
                write!(f, " : {}", self.body)
            }
        }
    }
}

/// The syntax style for a lambda function.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum LambdaSyntax {
    /// Arrow syntax: `param -> expr` or `(param1, param2) -> expr`
    Arrow,
    /// Lambda keyword syntax: `lambda param : expr` or `lambda param1, param2 : expr`
    LambdaKeyword,
}

/// Encapsulates the common pattern in SQL where either one unparenthesized item
/// such as an identifier or expression is permitted, or multiple of the same
/// item in a parenthesized list. For accessing items regardless of the form,
/// `OneOrManyWithParens` implements `Deref<Target = [T]>` and `IntoIterator`,
/// so you can call slice methods on it and iterate over items
/// # Examples
/// Accessing as a slice:
/// ```
/// # use sqlparser::ast::OneOrManyWithParens;
/// let one = OneOrManyWithParens::One("a");
///
/// assert_eq!(one[0], "a");
/// assert_eq!(one.len(), 1);
/// ```
/// Iterating:
/// ```
/// # use sqlparser::ast::OneOrManyWithParens;
/// let one = OneOrManyWithParens::One("a");
/// let many = OneOrManyWithParens::Many(vec!["a", "b"]);
///
/// assert_eq!(one.into_iter().chain(many).collect::<Vec<_>>(), vec!["a", "a", "b"] );
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum OneOrManyWithParens<T> {
    /// A single `T`, unparenthesized.
    One(T),
    /// One or more `T`s, parenthesized.
    Many(Vec<T>),
}

impl<T> Deref for OneOrManyWithParens<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        match self {
            OneOrManyWithParens::One(one) => core::slice::from_ref(one),
            OneOrManyWithParens::Many(many) => many,
        }
    }
}

impl<T> AsRef<[T]> for OneOrManyWithParens<T> {
    fn as_ref(&self) -> &[T] {
        self
    }
}

impl<'a, T> IntoIterator for &'a OneOrManyWithParens<T> {
    type Item = &'a T;
    type IntoIter = core::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Owned iterator implementation of `OneOrManyWithParens`
#[derive(Debug, Clone)]
pub struct OneOrManyWithParensIntoIter<T> {
    inner: OneOrManyWithParensIntoIterInner<T>,
}

#[derive(Debug, Clone)]
enum OneOrManyWithParensIntoIterInner<T> {
    One(core::iter::Once<T>),
    Many(<Vec<T> as IntoIterator>::IntoIter),
}

impl<T> core::iter::FusedIterator for OneOrManyWithParensIntoIter<T>
where
    core::iter::Once<T>: core::iter::FusedIterator,
    <Vec<T> as IntoIterator>::IntoIter: core::iter::FusedIterator,
{
}

impl<T> core::iter::ExactSizeIterator for OneOrManyWithParensIntoIter<T>
where
    core::iter::Once<T>: core::iter::ExactSizeIterator,
    <Vec<T> as IntoIterator>::IntoIter: core::iter::ExactSizeIterator,
{
}

impl<T> core::iter::Iterator for OneOrManyWithParensIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            OneOrManyWithParensIntoIterInner::One(one) => one.next(),
            OneOrManyWithParensIntoIterInner::Many(many) => many.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            OneOrManyWithParensIntoIterInner::One(one) => one.size_hint(),
            OneOrManyWithParensIntoIterInner::Many(many) => many.size_hint(),
        }
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        match self.inner {
            OneOrManyWithParensIntoIterInner::One(one) => one.count(),
            OneOrManyWithParensIntoIterInner::Many(many) => many.count(),
        }
    }

    fn fold<B, F>(mut self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        match &mut self.inner {
            OneOrManyWithParensIntoIterInner::One(one) => one.fold(init, f),
            OneOrManyWithParensIntoIterInner::Many(many) => many.fold(init, f),
        }
    }
}

impl<T> core::iter::DoubleEndedIterator for OneOrManyWithParensIntoIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            OneOrManyWithParensIntoIterInner::One(one) => one.next_back(),
            OneOrManyWithParensIntoIterInner::Many(many) => many.next_back(),
        }
    }
}

impl<T> IntoIterator for OneOrManyWithParens<T> {
    type Item = T;

    type IntoIter = OneOrManyWithParensIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        let inner = match self {
            OneOrManyWithParens::One(one) => {
                OneOrManyWithParensIntoIterInner::One(core::iter::once(one))
            }
            OneOrManyWithParens::Many(many) => {
                OneOrManyWithParensIntoIterInner::Many(many.into_iter())
            }
        };

        OneOrManyWithParensIntoIter { inner }
    }
}

impl<T> fmt::Display for OneOrManyWithParens<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OneOrManyWithParens::One(value) => write!(f, "{value}"),
            OneOrManyWithParens::Many(values) => {
                write!(f, "({})", display_comma_separated(values))
            }
        }
    }
}

impl fmt::Display for CastFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CastFormat::Value(v) => write!(f, "{v}"),
            CastFormat::ValueAtTimeZone(v, tz) => write!(f, "{v} AT TIME ZONE {tz}"),
        }
    }
}

impl fmt::Display for Expr {
    #[cfg_attr(feature = "recursive-protection", recursive::recursive)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Identifier(s) => write!(f, "{s}"),
            Expr::Wildcard(_) => f.write_str("*"),
            Expr::QualifiedWildcard(prefix, _) => write!(f, "{prefix}.*"),
            Expr::CompoundIdentifier(s) => write!(f, "{}", display_separated(s, ".")),
            Expr::CompoundFieldAccess { root, access_chain } => {
                write!(f, "{root}")?;
                for field in access_chain {
                    write!(f, "{field}")?;
                }
                Ok(())
            }
            Expr::IsTrue(ast) => write!(f, "{ast} IS TRUE"),
            Expr::IsNotTrue(ast) => write!(f, "{ast} IS NOT TRUE"),
            Expr::IsFalse(ast) => write!(f, "{ast} IS FALSE"),
            Expr::IsNotFalse(ast) => write!(f, "{ast} IS NOT FALSE"),
            Expr::IsNull(ast) => write!(f, "{ast} IS NULL"),
            Expr::IsNotNull(ast) => write!(f, "{ast} IS NOT NULL"),
            Expr::IsUnknown(ast) => write!(f, "{ast} IS UNKNOWN"),
            Expr::IsNotUnknown(ast) => write!(f, "{ast} IS NOT UNKNOWN"),
            Expr::InList {
                expr,
                list,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                display_comma_separated(list)
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                subquery
            ),
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => write!(
                f,
                "{} {}IN UNNEST({})",
                expr,
                if *negated { "NOT " } else { "" },
                array_expr
            ),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => write!(
                f,
                "{} {}BETWEEN {} AND {}",
                expr,
                if *negated { "NOT " } else { "" },
                low,
                high
            ),
            Expr::BinaryOp { left, op, right } => write!(f, "{left} {op} {right}"),
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
                any,
            } => match escape_char {
                Some(ch) => write!(
                    f,
                    "{} {}LIKE {}{} ESCAPE {}",
                    expr,
                    if *negated { "NOT " } else { "" },
                    if *any { "ANY " } else { "" },
                    pattern,
                    ch
                ),
                _ => write!(
                    f,
                    "{} {}LIKE {}{}",
                    expr,
                    if *negated { "NOT " } else { "" },
                    if *any { "ANY " } else { "" },
                    pattern
                ),
            },
            Expr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
                any,
            } => match escape_char {
                Some(ch) => write!(
                    f,
                    "{} {}ILIKE {}{} ESCAPE {}",
                    expr,
                    if *negated { "NOT " } else { "" },
                    if *any { "ANY" } else { "" },
                    pattern,
                    ch
                ),
                _ => write!(
                    f,
                    "{} {}ILIKE {}{}",
                    expr,
                    if *negated { "NOT " } else { "" },
                    if *any { "ANY " } else { "" },
                    pattern
                ),
            },
            Expr::RLike {
                negated,
                expr,
                pattern,
                regexp,
            } => write!(
                f,
                "{} {}{} {}",
                expr,
                if *negated { "NOT " } else { "" },
                if *regexp { "REGEXP" } else { "RLIKE" },
                pattern
            ),
            Expr::IsNormalized {
                expr,
                form,
                negated,
            } => {
                let not_ = if *negated { "NOT " } else { "" };
                if form.is_none() {
                    write!(f, "{expr} IS {not_}NORMALIZED")
                } else {
                    write!(
                        f,
                        "{} IS {}{} NORMALIZED",
                        expr,
                        not_,
                        form.as_ref().unwrap()
                    )
                }
            }
            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => match escape_char {
                Some(ch) => write!(
                    f,
                    "{} {}SIMILAR TO {} ESCAPE {}",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern,
                    ch
                ),
                _ => write!(
                    f,
                    "{} {}SIMILAR TO {}",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern
                ),
            },
            Expr::AnyOp {
                left,
                compare_op,
                right,
                is_some,
            } => {
                let add_parens = !matches!(right.as_ref(), Expr::Subquery(_));
                write!(
                    f,
                    "{left} {compare_op} {}{}{right}{}",
                    if *is_some { "SOME" } else { "ANY" },
                    if add_parens { "(" } else { "" },
                    if add_parens { ")" } else { "" },
                )
            }
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => {
                let add_parens = !matches!(right.as_ref(), Expr::Subquery(_));
                write!(
                    f,
                    "{left} {compare_op} ALL{}{right}{}",
                    if add_parens { "(" } else { "" },
                    if add_parens { ")" } else { "" },
                )
            }
            Expr::UnaryOp { op, expr } => {
                if op == &UnaryOperator::PGPostfixFactorial {
                    write!(f, "{expr}{op}")
                } else if matches!(
                    op,
                    UnaryOperator::Not
                        | UnaryOperator::Hash
                        | UnaryOperator::AtDashAt
                        | UnaryOperator::DoubleAt
                        | UnaryOperator::QuestionDash
                        | UnaryOperator::QuestionPipe
                ) {
                    write!(f, "{op} {expr}")
                } else {
                    write!(f, "{op}{expr}")
                }
            }
            Expr::Convert {
                is_try,
                expr,
                target_before_value,
                data_type,
                charset,
                styles,
            } => {
                write!(f, "{}CONVERT(", if *is_try { "TRY_" } else { "" })?;
                if let Some(data_type) = data_type {
                    if let Some(charset) = charset {
                        write!(f, "{expr}, {data_type} CHARACTER SET {charset}")
                    } else if *target_before_value {
                        write!(f, "{data_type}, {expr}")
                    } else {
                        write!(f, "{expr}, {data_type}")
                    }
                } else if let Some(charset) = charset {
                    write!(f, "{expr} USING {charset}")
                } else {
                    write!(f, "{expr}") // This should never happen
                }?;
                if !styles.is_empty() {
                    write!(f, ", {}", display_comma_separated(styles))?;
                }
                write!(f, ")")
            }
            Expr::Cast {
                kind,
                expr,
                data_type,
                format,
            } => match kind {
                CastKind::Cast => {
                    if let Some(format) = format {
                        write!(f, "CAST({expr} AS {data_type} FORMAT {format})")
                    } else {
                        write!(f, "CAST({expr} AS {data_type})")
                    }
                }
                CastKind::TryCast => {
                    if let Some(format) = format {
                        write!(f, "TRY_CAST({expr} AS {data_type} FORMAT {format})")
                    } else {
                        write!(f, "TRY_CAST({expr} AS {data_type})")
                    }
                }
                CastKind::SafeCast => {
                    if let Some(format) = format {
                        write!(f, "SAFE_CAST({expr} AS {data_type} FORMAT {format})")
                    } else {
                        write!(f, "SAFE_CAST({expr} AS {data_type})")
                    }
                }
                CastKind::DoubleColon => {
                    write!(f, "{expr}::{data_type}")
                }
            },
            Expr::Extract {
                field,
                syntax,
                expr,
            } => match syntax {
                ExtractSyntax::From => write!(f, "EXTRACT({field} FROM {expr})"),
                ExtractSyntax::Comma => write!(f, "EXTRACT({field}, {expr})"),
            },
            Expr::Ceil { expr, field } => match field {
                CeilFloorKind::DateTimeField(DateTimeField::NoDateTime) => {
                    write!(f, "CEIL({expr})")
                }
                CeilFloorKind::DateTimeField(dt_field) => write!(f, "CEIL({expr} TO {dt_field})"),
                CeilFloorKind::Scale(s) => write!(f, "CEIL({expr}, {s})"),
            },
            Expr::Floor { expr, field } => match field {
                CeilFloorKind::DateTimeField(DateTimeField::NoDateTime) => {
                    write!(f, "FLOOR({expr})")
                }
                CeilFloorKind::DateTimeField(dt_field) => write!(f, "FLOOR({expr} TO {dt_field})"),
                CeilFloorKind::Scale(s) => write!(f, "FLOOR({expr}, {s})"),
            },
            Expr::Position { expr, r#in } => write!(f, "POSITION({expr} IN {in})"),
            Expr::Collate { expr, collation } => write!(f, "{expr} COLLATE {collation}"),
            Expr::Nested(ast) => write!(f, "({ast})"),
            Expr::Value(v) => write!(f, "{v}"),
            Expr::Prefixed { prefix, value } => write!(f, "{prefix} {value}"),
            Expr::TypedString(ts) => ts.fmt(f),
            Expr::Function(fun) => fun.fmt(f),
            Expr::Case {
                case_token: _,
                end_token: _,
                operand,
                conditions,
                else_result,
            } => {
                f.write_str("CASE")?;
                if let Some(operand) = operand {
                    f.write_str(" ")?;
                    operand.fmt(f)?;
                }
                for when in conditions {
                    SpaceOrNewline.fmt(f)?;
                    Indent(when).fmt(f)?;
                }
                if let Some(else_result) = else_result {
                    SpaceOrNewline.fmt(f)?;
                    Indent("ELSE").fmt(f)?;
                    SpaceOrNewline.fmt(f)?;
                    Indent(Indent(else_result)).fmt(f)?;
                }
                SpaceOrNewline.fmt(f)?;
                f.write_str("END")
            }
            Expr::Exists { subquery, negated } => write!(
                f,
                "{}EXISTS ({})",
                if *negated { "NOT " } else { "" },
                subquery
            ),
            Expr::Subquery(s) => write!(f, "({s})"),
            Expr::GroupingSets(sets) => {
                write!(f, "GROUPING SETS (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{sep}")?;
                    sep = ", ";
                    write!(f, "({})", display_comma_separated(set))?;
                }
                write!(f, ")")
            }
            Expr::Cube(sets) => {
                write!(f, "CUBE (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{sep}")?;
                    sep = ", ";
                    if set.len() == 1 {
                        write!(f, "{}", set[0])?;
                    } else {
                        write!(f, "({})", display_comma_separated(set))?;
                    }
                }
                write!(f, ")")
            }
            Expr::Rollup(sets) => {
                write!(f, "ROLLUP (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{sep}")?;
                    sep = ", ";
                    if set.len() == 1 {
                        write!(f, "{}", set[0])?;
                    } else {
                        write!(f, "({})", display_comma_separated(set))?;
                    }
                }
                write!(f, ")")
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                special,
                shorthand,
            } => {
                f.write_str("SUBSTR")?;
                if !*shorthand {
                    f.write_str("ING")?;
                }
                write!(f, "({expr}")?;
                if let Some(from_part) = substring_from {
                    if *special {
                        write!(f, ", {from_part}")?;
                    } else {
                        write!(f, " FROM {from_part}")?;
                    }
                }
                if let Some(for_part) = substring_for {
                    if *special {
                        write!(f, ", {for_part}")?;
                    } else {
                        write!(f, " FOR {for_part}")?;
                    }
                }

                write!(f, ")")
            }
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                write!(
                    f,
                    "OVERLAY({expr} PLACING {overlay_what} FROM {overlay_from}"
                )?;
                if let Some(for_part) = overlay_for {
                    write!(f, " FOR {for_part}")?;
                }

                write!(f, ")")
            }
            Expr::IsDistinctFrom(a, b) => write!(f, "{a} IS DISTINCT FROM {b}"),
            Expr::IsNotDistinctFrom(a, b) => write!(f, "{a} IS NOT DISTINCT FROM {b}"),
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => {
                write!(f, "TRIM(")?;
                if let Some(ident) = trim_where {
                    write!(f, "{ident} ")?;
                }
                if let Some(trim_char) = trim_what {
                    write!(f, "{trim_char} FROM {expr}")?;
                } else {
                    write!(f, "{expr}")?;
                }
                if let Some(characters) = trim_characters {
                    write!(f, ", {}", display_comma_separated(characters))?;
                }

                write!(f, ")")
            }
            Expr::Tuple(exprs) => {
                write!(f, "({})", display_comma_separated(exprs))
            }
            Expr::Struct { values, fields } => {
                if !fields.is_empty() {
                    write!(
                        f,
                        "STRUCT<{}>({})",
                        display_comma_separated(fields),
                        display_comma_separated(values)
                    )
                } else {
                    write!(f, "STRUCT({})", display_comma_separated(values))
                }
            }
            Expr::Named { expr, name } => {
                write!(f, "{expr} AS {name}")
            }
            Expr::Dictionary(fields) => {
                write!(f, "{{{}}}", display_comma_separated(fields))
            }
            Expr::Map(map) => {
                write!(f, "{map}")
            }
            Expr::Array(set) => {
                write!(f, "{set}")
            }
            Expr::JsonAccess { value, path } => {
                write!(f, "{value}{path}")
            }
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => {
                write!(f, "{timestamp} AT TIME ZONE {time_zone}")
            }
            Expr::Interval(interval) => {
                write!(f, "{interval}")
            }
            Expr::MatchAgainst {
                columns,
                match_value: match_expr,
                opt_search_modifier,
            } => {
                write!(f, "MATCH ({}) AGAINST ", display_comma_separated(columns),)?;

                if let Some(search_modifier) = opt_search_modifier {
                    write!(f, "({match_expr} {search_modifier})")?;
                } else {
                    write!(f, "({match_expr})")?;
                }

                Ok(())
            }
            Expr::OuterJoin(expr) => {
                write!(f, "{expr} (+)")
            }
            Expr::Prior(expr) => write!(f, "PRIOR {expr}"),
            Expr::Lambda(lambda) => write!(f, "{lambda}"),
            Expr::MemberOf(member_of) => write!(f, "{member_of}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum WindowType {
    WindowSpec(WindowSpec),
    NamedWindow(Ident),
}

impl Display for WindowType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowType::WindowSpec(spec) => {
                f.write_str("(")?;
                NewLine.fmt(f)?;
                Indent(spec).fmt(f)?;
                NewLine.fmt(f)?;
                f.write_str(")")
            }
            WindowType::NamedWindow(name) => name.fmt(f),
        }
    }
}

/// A window specification (i.e. `OVER ([window_name] PARTITION BY .. ORDER BY .. etc.)`)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct WindowSpec {
    /// Optional window name.
    ///
    /// You can find it at least in [MySQL][1], [BigQuery][2], [PostgreSQL][3]
    ///
    /// [1]: https://dev.mysql.com/doc/refman/8.0/en/window-functions-named-windows.html
    /// [2]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls
    /// [3]: https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS
    pub window_name: Option<Ident>,
    /// `OVER (PARTITION BY ...)`
    pub partition_by: Vec<Expr>,
    /// `OVER (ORDER BY ...)`
    pub order_by: Vec<OrderByExpr>,
    /// `OVER (window frame)`
    pub window_frame: Option<WindowFrame>,
}

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut is_first = true;
        if let Some(window_name) = &self.window_name {
            if !is_first {
                SpaceOrNewline.fmt(f)?;
            }
            is_first = false;
            write!(f, "{window_name}")?;
        }
        if !self.partition_by.is_empty() {
            if !is_first {
                SpaceOrNewline.fmt(f)?;
            }
            is_first = false;
            write!(
                f,
                "PARTITION BY {}",
                display_comma_separated(&self.partition_by)
            )?;
        }
        if !self.order_by.is_empty() {
            if !is_first {
                SpaceOrNewline.fmt(f)?;
            }
            is_first = false;
            write!(f, "ORDER BY {}", display_comma_separated(&self.order_by))?;
        }
        if let Some(window_frame) = &self.window_frame {
            if !is_first {
                SpaceOrNewline.fmt(f)?;
            }
            if let Some(end_bound) = &window_frame.end_bound {
                write!(
                    f,
                    "{} BETWEEN {} AND {}",
                    window_frame.units, window_frame.start_bound, end_bound
                )?;
            } else {
                write!(f, "{} {}", window_frame.units, window_frame.start_bound)?;
            }
        }
        Ok(())
    }
}

/// Specifies the data processed by a window function, e.g.
/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
///
/// Note: The parser does not validate the specified bounds; the caller should
/// reject invalid bounds like `ROWS UNBOUNDED FOLLOWING` before execution.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    /// The right bound of the `BETWEEN .. AND` clause. The end bound of `None`
    /// indicates the shorthand form (e.g. `ROWS 1 PRECEDING`), which must
    /// behave the same as `end_bound = WindowFrameBound::CurrentRow`.
    pub end_bound: Option<WindowFrameBound>,
    // TBD: EXCLUDE
}

impl Default for WindowFrame {
    /// Returns default value for window frame
    ///
    /// See [this page](https://www.sqlite.org/windowfunctions.html#frame_specifications) for more details.
    fn default() -> Self {
        Self {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(None),
            end_bound: None,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

impl fmt::Display for WindowFrameUnits {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })
    }
}

/// Specifies Ignore / Respect NULL within window functions.
/// For example
/// `FIRST_VALUE(column2) IGNORE NULLS OVER (PARTITION BY column1)`
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum NullTreatment {
    IgnoreNulls,
    RespectNulls,
}

impl fmt::Display for NullTreatment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            NullTreatment::IgnoreNulls => "IGNORE NULLS",
            NullTreatment::RespectNulls => "RESPECT NULLS",
        })
    }
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum WindowFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<Box<Expr>>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<Box<Expr>>),
}

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(Some(n)) => write!(f, "{n} PRECEDING"),
            WindowFrameBound::Following(Some(n)) => write!(f, "{n} FOLLOWING"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AddDropSync {
    ADD,
    DROP,
    SYNC,
}

impl fmt::Display for AddDropSync {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AddDropSync::SYNC => f.write_str("SYNC PARTITIONS"),
            AddDropSync::DROP => f.write_str("DROP PARTITIONS"),
            AddDropSync::ADD => f.write_str("ADD PARTITIONS"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ShowCreateObject {
    Event,
    Function,
    Procedure,
    Table,
    Trigger,
    View,
}

impl fmt::Display for ShowCreateObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ShowCreateObject::Event => f.write_str("EVENT"),
            ShowCreateObject::Function => f.write_str("FUNCTION"),
            ShowCreateObject::Procedure => f.write_str("PROCEDURE"),
            ShowCreateObject::Table => f.write_str("TABLE"),
            ShowCreateObject::Trigger => f.write_str("TRIGGER"),
            ShowCreateObject::View => f.write_str("VIEW"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CommentObject {
    Column,
    Table,
    Extension,
    Schema,
    Database,
    User,
    Role,
}

impl fmt::Display for CommentObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CommentObject::Column => f.write_str("COLUMN"),
            CommentObject::Table => f.write_str("TABLE"),
            CommentObject::Extension => f.write_str("EXTENSION"),
            CommentObject::Schema => f.write_str("SCHEMA"),
            CommentObject::Database => f.write_str("DATABASE"),
            CommentObject::User => f.write_str("USER"),
            CommentObject::Role => f.write_str("ROLE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Password {
    Password(Expr),
    NullPassword,
}

/// A `CASE` statement.
///
/// Examples:
/// ```sql
/// CASE
///     WHEN EXISTS(SELECT 1)
///         THEN SELECT 1 FROM T;
///     WHEN EXISTS(SELECT 2)
///         THEN SELECT 1 FROM U;
///     ELSE
///         SELECT 1 FROM V;
/// END CASE;
/// ```
///
/// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#case_search_expression)
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/snowflake-scripting/case)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CaseStatement {
    /// The `CASE` token that starts the statement.
    pub case_token: AttachedToken,
    pub match_expr: Option<Expr>,
    pub when_blocks: Vec<ConditionalStatementBlock>,
    pub else_block: Option<ConditionalStatementBlock>,
    /// The last token of the statement (`END` or `CASE`).
    pub end_case_token: AttachedToken,
}

impl fmt::Display for CaseStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let CaseStatement {
            case_token: _,
            match_expr,
            when_blocks,
            else_block,
            end_case_token: AttachedToken(end),
        } = self;

        write!(f, "CASE")?;

        if let Some(expr) = match_expr {
            write!(f, " {expr}")?;
        }

        if !when_blocks.is_empty() {
            write!(f, " {}", display_separated(when_blocks, " "))?;
        }

        if let Some(else_block) = else_block {
            write!(f, " {else_block}")?;
        }

        write!(f, " END")?;

        if let Token::Word(w) = &end.token {
            if w.keyword == Keyword::CASE {
                write!(f, " CASE")?;
            }
        }

        Ok(())
    }
}

/// An `IF` statement.
///
/// Example (BigQuery or Snowflake):
/// ```sql
/// IF TRUE THEN
///     SELECT 1;
///     SELECT 2;
/// ELSEIF TRUE THEN
///     SELECT 3;
/// ELSE
///     SELECT 4;
/// END IF
/// ```
/// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#if)
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/snowflake-scripting/if)
///
/// Example (MSSQL):
/// ```sql
/// IF 1=1 SELECT 1 ELSE SELECT 2
/// ```
/// [MSSQL](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/if-else-transact-sql?view=sql-server-ver16)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IfStatement {
    pub if_block: ConditionalStatementBlock,
    pub elseif_blocks: Vec<ConditionalStatementBlock>,
    pub else_block: Option<ConditionalStatementBlock>,
    pub end_token: Option<AttachedToken>,
}

impl fmt::Display for IfStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let IfStatement {
            if_block,
            elseif_blocks,
            else_block,
            end_token,
        } = self;

        write!(f, "{if_block}")?;

        for elseif_block in elseif_blocks {
            write!(f, " {elseif_block}")?;
        }

        if let Some(else_block) = else_block {
            write!(f, " {else_block}")?;
        }

        if let Some(AttachedToken(end_token)) = end_token {
            write!(f, " END {end_token}")?;
        }

        Ok(())
    }
}

/// A `WHILE` statement.
///
/// Example:
/// ```sql
/// WHILE @@FETCH_STATUS = 0
/// BEGIN
///    FETCH NEXT FROM c1 INTO @var1, @var2;
/// END
/// ```
///
/// [MsSql](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/while-transact-sql)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct WhileStatement {
    pub while_block: ConditionalStatementBlock,
}

impl fmt::Display for WhileStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let WhileStatement { while_block } = self;
        write!(f, "{while_block}")?;
        Ok(())
    }
}

/// A block within a [Statement::Case] or [Statement::If] or [Statement::While]-like statement
///
/// Example 1:
/// ```sql
/// WHEN EXISTS(SELECT 1) THEN SELECT 1;
/// ```
///
/// Example 2:
/// ```sql
/// IF TRUE THEN SELECT 1; SELECT 2;
/// ```
///
/// Example 3:
/// ```sql
/// ELSE SELECT 1; SELECT 2;
/// ```
///
/// Example 4:
/// ```sql
/// WHILE @@FETCH_STATUS = 0
/// BEGIN
///    FETCH NEXT FROM c1 INTO @var1, @var2;
/// END
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ConditionalStatementBlock {
    pub start_token: AttachedToken,
    pub condition: Option<Expr>,
    pub then_token: Option<AttachedToken>,
    pub conditional_statements: ConditionalStatements,
}

impl ConditionalStatementBlock {
    pub fn statements(&self) -> &Vec<Statement> {
        self.conditional_statements.statements()
    }
}

impl fmt::Display for ConditionalStatementBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ConditionalStatementBlock {
            start_token: AttachedToken(start_token),
            condition,
            then_token,
            conditional_statements,
        } = self;

        write!(f, "{start_token}")?;

        if let Some(condition) = condition {
            write!(f, " {condition}")?;
        }

        if then_token.is_some() {
            write!(f, " THEN")?;
        }

        if !conditional_statements.statements().is_empty() {
            write!(f, " {conditional_statements}")?;
        }

        Ok(())
    }
}

/// A list of statements in a [ConditionalStatementBlock].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ConditionalStatements {
    /// SELECT 1; SELECT 2; SELECT 3; ...
    Sequence { statements: Vec<Statement> },
    /// BEGIN SELECT 1; SELECT 2; SELECT 3; ... END
    BeginEnd(BeginEndStatements),
}

impl ConditionalStatements {
    pub fn statements(&self) -> &Vec<Statement> {
        match self {
            ConditionalStatements::Sequence { statements } => statements,
            ConditionalStatements::BeginEnd(bes) => &bes.statements,
        }
    }
}

impl fmt::Display for ConditionalStatements {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConditionalStatements::Sequence { statements } => {
                if !statements.is_empty() {
                    format_statement_list(f, statements)?;
                }
                Ok(())
            }
            ConditionalStatements::BeginEnd(bes) => write!(f, "{bes}"),
        }
    }
}

/// Represents a list of statements enclosed within `BEGIN` and `END` keywords.
/// Example:
/// ```sql
/// BEGIN
///     SELECT 1;
///     SELECT 2;
/// END
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct BeginEndStatements {
    pub begin_token: AttachedToken,
    pub statements: Vec<Statement>,
    pub end_token: AttachedToken,
}

impl fmt::Display for BeginEndStatements {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let BeginEndStatements {
            begin_token: AttachedToken(begin_token),
            statements,
            end_token: AttachedToken(end_token),
        } = self;

        if begin_token.token != Token::EOF {
            write!(f, "{begin_token} ")?;
        }
        if !statements.is_empty() {
            format_statement_list(f, statements)?;
        }
        if end_token.token != Token::EOF {
            write!(f, " {end_token}")?;
        }
        Ok(())
    }
}

/// A `RAISE` statement.
///
/// Examples:
/// ```sql
/// RAISE USING MESSAGE = 'error';
///
/// RAISE myerror;
/// ```
///
/// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#raise)
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/snowflake-scripting/raise)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RaiseStatement {
    pub value: Option<RaiseStatementValue>,
}

impl fmt::Display for RaiseStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let RaiseStatement { value } = self;

        write!(f, "RAISE")?;
        if let Some(value) = value {
            write!(f, " {value}")?;
        }

        Ok(())
    }
}

/// Represents the error value of a [RaiseStatement].
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RaiseStatementValue {
    /// `RAISE USING MESSAGE = 'error'`
    UsingMessage(Expr),
    /// `RAISE myerror`
    Expr(Expr),
}

impl fmt::Display for RaiseStatementValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaiseStatementValue::Expr(expr) => write!(f, "{expr}"),
            RaiseStatementValue::UsingMessage(expr) => write!(f, "USING MESSAGE = {expr}"),
        }
    }
}

/// Represents an expression assignment within a variable `DECLARE` statement.
///
/// Examples:
/// ```sql
/// DECLARE variable_name := 42
/// DECLARE variable_name DEFAULT 42
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DeclareAssignment {
    /// Plain expression specified.
    Expr(Box<Expr>),

    /// Expression assigned via the `DEFAULT` keyword
    Default(Box<Expr>),

    /// Expression assigned via the `:=` syntax
    ///
    /// Example:
    /// ```sql
    /// DECLARE variable_name := 42;
    /// ```
    DuckAssignment(Box<Expr>),

    /// Expression via the `FOR` keyword
    ///
    /// Example:
    /// ```sql
    /// DECLARE c1 CURSOR FOR res
    /// ```
    For(Box<Expr>),

    /// Expression via the `=` syntax.
    ///
    /// Example:
    /// ```sql
    /// DECLARE @variable AS INT = 100
    /// ```
    MsSqlAssignment(Box<Expr>),
}

impl fmt::Display for DeclareAssignment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DeclareAssignment::Expr(expr) => {
                write!(f, "{expr}")
            }
            DeclareAssignment::Default(expr) => {
                write!(f, "DEFAULT {expr}")
            }
            DeclareAssignment::DuckAssignment(expr) => {
                write!(f, ":= {expr}")
            }
            DeclareAssignment::MsSqlAssignment(expr) => {
                write!(f, "= {expr}")
            }
            DeclareAssignment::For(expr) => {
                write!(f, "FOR {expr}")
            }
        }
    }
}

/// Represents the type of a `DECLARE` statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DeclareType {
    /// Cursor variable type. e.g. [Snowflake] [PostgreSQL] [MsSql]
    ///
    /// [Snowflake]: https://docs.snowflake.com/en/developer-guide/snowflake-scripting/cursors#declaring-a-cursor
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/plpgsql-cursors.html
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/language-elements/declare-cursor-transact-sql
    Cursor,

    /// Result set variable type. [Snowflake]
    ///
    /// Syntax:
    /// ```text
    /// <resultset_name> RESULTSET [ { DEFAULT | := } ( <query> ) ] ;
    /// ```
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/snowflake-scripting/declare#resultset-declaration-syntax
    ResultSet,

    /// Exception declaration syntax. [Snowflake]
    ///
    /// Syntax:
    /// ```text
    /// <exception_name> EXCEPTION [ ( <exception_number> , '<exception_message>' ) ] ;
    /// ```
    /// [Snowflake]: https://docs.snowflake.com/en/sql-reference/snowflake-scripting/declare#exception-declaration-syntax
    Exception,
}

impl fmt::Display for DeclareType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DeclareType::Cursor => {
                write!(f, "CURSOR")
            }
            DeclareType::ResultSet => {
                write!(f, "RESULTSET")
            }
            DeclareType::Exception => {
                write!(f, "EXCEPTION")
            }
        }
    }
}

/// A `DECLARE` statement.
/// [PostgreSQL] [Snowflake] [BigQuery]
///
/// Examples:
/// ```sql
/// DECLARE variable_name := 42
/// DECLARE liahona CURSOR FOR SELECT * FROM films;
/// ```
///
/// [PostgreSQL]: https://www.postgresql.org/docs/current/sql-declare.html
/// [Snowflake]: https://docs.snowflake.com/en/sql-reference/snowflake-scripting/declare
/// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#declare
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Declare {
    /// The name(s) being declared.
    /// Example: `DECLARE a, b, c DEFAULT 42;
    pub names: Vec<Ident>,
    /// Data-type assigned to the declared variable.
    /// Example: `DECLARE x INT64 DEFAULT 42;
    pub data_type: Option<DataType>,
    /// Expression being assigned to the declared variable.
    pub assignment: Option<DeclareAssignment>,
    /// Represents the type of the declared variable.
    pub declare_type: Option<DeclareType>,
    /// Causes the cursor to return data in binary rather than in text format.
    pub binary: Option<bool>,
    /// None = Not specified
    /// Some(true) = INSENSITIVE
    /// Some(false) = ASENSITIVE
    pub sensitive: Option<bool>,
    /// None = Not specified
    /// Some(true) = SCROLL
    /// Some(false) = NO SCROLL
    pub scroll: Option<bool>,
    /// None = Not specified
    /// Some(true) = WITH HOLD, specifies that the cursor can continue to be used after the transaction that created it successfully commits
    /// Some(false) = WITHOUT HOLD, specifies that the cursor cannot be used outside of the transaction that created it
    pub hold: Option<bool>,
    /// `FOR <query>` clause in a CURSOR declaration.
    pub for_query: Option<Box<Query>>,
}

impl fmt::Display for Declare {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Declare {
            names,
            data_type,
            assignment,
            declare_type,
            binary,
            sensitive,
            scroll,
            hold,
            for_query,
        } = self;
        write!(f, "{}", display_comma_separated(names))?;

        if let Some(true) = binary {
            write!(f, " BINARY")?;
        }

        if let Some(sensitive) = sensitive {
            if *sensitive {
                write!(f, " INSENSITIVE")?;
            } else {
                write!(f, " ASENSITIVE")?;
            }
        }

        if let Some(scroll) = scroll {
            if *scroll {
                write!(f, " SCROLL")?;
            } else {
                write!(f, " NO SCROLL")?;
            }
        }

        if let Some(declare_type) = declare_type {
            write!(f, " {declare_type}")?;
        }

        if let Some(hold) = hold {
            if *hold {
                write!(f, " WITH HOLD")?;
            } else {
                write!(f, " WITHOUT HOLD")?;
            }
        }

        if let Some(query) = for_query {
            write!(f, " FOR {query}")?;
        }

        if let Some(data_type) = data_type {
            write!(f, " {data_type}")?;
        }

        if let Some(expr) = assignment {
            write!(f, " {expr}")?;
        }
        Ok(())
    }
}

/// Sql options of a `CREATE TABLE` statement.
#[derive(Debug, Default, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CreateTableOptions {
    #[default]
    None,
    /// Options specified using the `WITH` keyword.
    /// e.g. `WITH (description = "123")`
    ///
    /// <https://www.postgresql.org/docs/current/sql-createtable.html>
    ///
    /// MSSQL supports more specific options that's not only key-value pairs.
    ///
    /// WITH (
    ///     DISTRIBUTION = ROUND_ROBIN,
    ///     CLUSTERED INDEX (column_a DESC, column_b)
    /// )
    ///
    /// <https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse?view=aps-pdw-2016-au7#syntax>
    With(Vec<SqlOption>),
    /// Options specified using the `OPTIONS` keyword.
    /// e.g. `OPTIONS(description = "123")`
    ///
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#table_option_list>
    Options(Vec<SqlOption>),

    /// Plain options, options which are not part on any declerative statement e.g. WITH/OPTIONS/...
    /// <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
    Plain(Vec<SqlOption>),

    TableProperties(Vec<SqlOption>),
}

impl fmt::Display for CreateTableOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CreateTableOptions::With(with_options) => {
                write!(f, "WITH ({})", display_comma_separated(with_options))
            }
            CreateTableOptions::Options(options) => {
                write!(f, "OPTIONS({})", display_comma_separated(options))
            }
            CreateTableOptions::TableProperties(options) => {
                write!(f, "TBLPROPERTIES ({})", display_comma_separated(options))
            }
            CreateTableOptions::Plain(options) => {
                write!(f, "{}", display_separated(options, " "))
            }
            CreateTableOptions::None => Ok(()),
        }
    }
}

/// A `FROM` clause within a `DELETE` statement.
///
/// Syntax
/// ```sql
/// [FROM] table
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FromTable {
    /// An explicit `FROM` keyword was specified.
    WithFromKeyword(Vec<TableWithJoins>),
    /// BigQuery: `FROM` keyword was omitted.
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_statement>
    WithoutKeyword(Vec<TableWithJoins>),
}
impl Display for FromTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FromTable::WithFromKeyword(tables) => {
                write!(f, "FROM {}", display_comma_separated(tables))
            }
            FromTable::WithoutKeyword(tables) => {
                write!(f, "{}", display_comma_separated(tables))
            }
        }
    }
}

/// Policy type for a `CREATE POLICY` statement.
/// ```sql
/// AS [ PERMISSIVE | RESTRICTIVE ]
/// ```
/// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createpolicy.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CreatePolicyType {
    Permissive,
    Restrictive,
}

/// Policy command for a `CREATE POLICY` statement.
/// ```sql
/// FOR [ALL | SELECT | INSERT | UPDATE | DELETE]
/// ```
/// [PostgreSQL](https://www.postgresql.org/docs/current/sql-createpolicy.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CreatePolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Set {
    /// SQL Standard-style
    /// SET a = 1;
    SingleAssignment {
        scope: Option<ContextModifier>,
        hivevar: bool,
        variable: ObjectName,
        values: Vec<Expr>,
    },
    /// Snowflake-style
    /// SET (a, b, ..) = (1, 2, ..);
    ParenthesizedAssignments {
        variables: Vec<ObjectName>,
        values: Vec<Expr>,
    },
    /// MySQL-style
    /// SET a = 1, b = 2, ..;
    MultipleAssignments { assignments: Vec<SetAssignment> },
    /// Session authorization for Postgres/Redshift
    ///
    /// ```sql
    /// SET SESSION AUTHORIZATION { user_name | DEFAULT }
    /// ```
    ///
    /// See <https://www.postgresql.org/docs/current/sql-set-session-authorization.html>
    /// See <https://docs.aws.amazon.com/redshift/latest/dg/r_SET_SESSION_AUTHORIZATION.html>
    SetSessionAuthorization(SetSessionAuthorizationParam),
    /// MS-SQL session
    ///
    /// See <https://learn.microsoft.com/en-us/sql/t-sql/statements/set-statements-transact-sql>
    SetSessionParam(SetSessionParamKind),
    /// ```sql
    /// SET [ SESSION | LOCAL ] ROLE role_name
    /// ```
    ///
    /// Sets session state. Examples: [ANSI][1], [Postgresql][2], [MySQL][3], and [Oracle][4]
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#set-role-statement
    /// [2]: https://www.postgresql.org/docs/14/sql-set-role.html
    /// [3]: https://dev.mysql.com/doc/refman/8.0/en/set-role.html
    /// [4]: https://docs.oracle.com/cd/B19306_01/server.102/b14200/statements_10004.htm
    SetRole {
        /// Non-ANSI optional identifier to inform if the role is defined inside the current session (`SESSION`) or transaction (`LOCAL`).
        context_modifier: Option<ContextModifier>,
        /// Role name. If NONE is specified, then the current role name is removed.
        role_name: Option<Ident>,
    },
    /// ```sql
    /// SET TIME ZONE <value>
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statements
    /// `SET TIME ZONE <value>` is an alias for `SET timezone TO <value>` in PostgreSQL
    /// However, we allow it for all dialects.
    SetTimeZone { local: bool, value: Expr },
    /// ```sql
    /// SET NAMES 'charset_name' [COLLATE 'collation_name']
    /// ```
    SetNames {
        charset_name: Ident,
        collation_name: Option<String>,
    },
    /// ```sql
    /// SET NAMES DEFAULT
    /// ```
    ///
    /// Note: this is a MySQL-specific statement.
    SetNamesDefault {},
    /// ```sql
    /// SET TRANSACTION ...
    /// ```
    SetTransaction {
        modes: Vec<TransactionMode>,
        snapshot: Option<Value>,
        session: bool,
    },
}

impl Display for Set {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ParenthesizedAssignments { variables, values } => write!(
                f,
                "SET ({}) = ({})",
                display_comma_separated(variables),
                display_comma_separated(values)
            ),
            Self::MultipleAssignments { assignments } => {
                write!(f, "SET {}", display_comma_separated(assignments))
            }
            Self::SetRole {
                context_modifier,
                role_name,
            } => {
                let role_name = role_name.clone().unwrap_or_else(|| Ident::new("NONE"));
                write!(
                    f,
                    "SET {modifier}ROLE {role_name}",
                    modifier = context_modifier.map(|m| format!("{m}")).unwrap_or_default()
                )
            }
            Self::SetSessionAuthorization(kind) => write!(f, "SET SESSION AUTHORIZATION {kind}"),
            Self::SetSessionParam(kind) => write!(f, "SET {kind}"),
            Self::SetTransaction {
                modes,
                snapshot,
                session,
            } => {
                if *session {
                    write!(f, "SET SESSION CHARACTERISTICS AS TRANSACTION")?;
                } else {
                    write!(f, "SET TRANSACTION")?;
                }
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                if let Some(snapshot_id) = snapshot {
                    write!(f, " SNAPSHOT {snapshot_id}")?;
                }
                Ok(())
            }
            Self::SetTimeZone { local, value } => {
                f.write_str("SET ")?;
                if *local {
                    f.write_str("LOCAL ")?;
                }
                write!(f, "TIME ZONE {value}")
            }
            Self::SetNames {
                charset_name,
                collation_name,
            } => {
                write!(f, "SET NAMES {charset_name}")?;

                if let Some(collation) = collation_name {
                    f.write_str(" COLLATE ")?;
                    f.write_str(collation)?;
                };

                Ok(())
            }
            Self::SetNamesDefault {} => {
                f.write_str("SET NAMES DEFAULT")?;

                Ok(())
            }
            Set::SingleAssignment {
                scope,
                hivevar,
                variable,
                values,
            } => {
                write!(
                    f,
                    "SET {}{}{} = {}",
                    scope.map(|s| format!("{s}")).unwrap_or_default(),
                    if *hivevar { "HIVEVAR:" } else { "" },
                    variable,
                    display_comma_separated(values)
                )
            }
        }
    }
}

/// A representation of a `WHEN` arm with all the identifiers catched and the statements to execute
/// for the arm.
///
/// Snowflake: <https://docs.snowflake.com/en/sql-reference/snowflake-scripting/exception>
/// BigQuery: <https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#beginexceptionend>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ExceptionWhen {
    pub idents: Vec<Ident>,
    pub statements: Vec<Statement>,
}

impl Display for ExceptionWhen {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "WHEN {idents} THEN",
            idents = display_separated(&self.idents, " OR ")
        )?;

        if !self.statements.is_empty() {
            write!(f, " ")?;
            format_statement_list(f, &self.statements)?;
        }

        Ok(())
    }
}

/// ANALYZE TABLE statement (Hive-specific)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Analyze {
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub table_name: ObjectName,
    pub partitions: Option<Vec<Expr>>,
    pub for_columns: bool,
    pub columns: Vec<Ident>,
    pub cache_metadata: bool,
    pub noscan: bool,
    pub compute_statistics: bool,
    pub has_table_keyword: bool,
}

impl fmt::Display for Analyze {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ANALYZE{}{table_name}",
            if self.has_table_keyword {
                " TABLE "
            } else {
                " "
            },
            table_name = self.table_name
        )?;
        if let Some(ref parts) = self.partitions {
            if !parts.is_empty() {
                write!(f, " PARTITION ({})", display_comma_separated(parts))?;
            }
        }

        if self.compute_statistics {
            write!(f, " COMPUTE STATISTICS")?;
        }
        if self.noscan {
            write!(f, " NOSCAN")?;
        }
        if self.cache_metadata {
            write!(f, " CACHE METADATA")?;
        }
        if self.for_columns {
            write!(f, " FOR COLUMNS")?;
            if !self.columns.is_empty() {
                write!(f, " {}", display_comma_separated(&self.columns))?;
            }
        }
        Ok(())
    }
}

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(
    feature = "visitor",
    derive(Visit, VisitMut),
    visit(with = "visit_statement")
)]
pub enum Statement {
    /// ```sql
    /// ANALYZE
    /// ```
    /// Analyze (Hive)
    Analyze(Analyze),
    Set(Set),
    /// ```sql
    /// TRUNCATE
    /// ```
    /// Truncate (Hive)
    Truncate(Truncate),
    /// ```sql
    /// MSCK
    /// ```
    /// Msck (Hive)
    Msck(Msck),
    /// ```sql
    /// SELECT
    /// ```
    Query(Box<Query>),
    /// ```sql
    /// INSERT
    /// ```
    Insert(Insert),
    /// ```sql
    /// INSTALL
    /// ```
    Install {
        /// Only for DuckDB
        extension_name: Ident,
    },
    /// ```sql
    /// LOAD
    /// ```
    Load {
        /// Only for DuckDB
        extension_name: Ident,
    },
    // TODO: Support ROW FORMAT
    Directory {
        overwrite: bool,
        local: bool,
        path: String,
        file_format: Option<FileFormat>,
        source: Box<Query>,
    },
    /// A `CASE` statement.
    Case(CaseStatement),
    /// An `IF` statement.
    If(IfStatement),
    /// A `WHILE` statement.
    While(WhileStatement),
    /// A `RAISE` statement.
    Raise(RaiseStatement),
    /// ```sql
    /// CALL <function>
    /// ```
    Call(Function),
    /// ```sql
    /// COPY [TO | FROM] ...
    /// ```
    Copy {
        /// The source of 'COPY TO', or the target of 'COPY FROM'
        source: CopySource,
        /// If true, is a 'COPY TO' statement. If false is a 'COPY FROM'
        to: bool,
        /// The target of 'COPY TO', or the source of 'COPY FROM'
        target: CopyTarget,
        /// WITH options (from PostgreSQL version 9.0)
        options: Vec<CopyOption>,
        /// WITH options (before PostgreSQL version 9.0)
        legacy_options: Vec<CopyLegacyOption>,
        /// VALUES a vector of values to be copied
        values: Vec<Option<String>>,
    },
    /// ```sql
    /// COPY INTO <table> | <location>
    /// ```
    /// See:
    /// <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table>
    /// <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location>
    ///
    /// Copy Into syntax available for Snowflake is different than the one implemented in
    /// Postgres. Although they share common prefix, it is reasonable to implement them
    /// in different enums. This can be refactored later once custom dialects
    /// are allowed to have custom Statements.
    CopyIntoSnowflake {
        kind: CopyIntoSnowflakeKind,
        into: ObjectName,
        into_columns: Option<Vec<Ident>>,
        from_obj: Option<ObjectName>,
        from_obj_alias: Option<Ident>,
        stage_params: StageParamsObject,
        from_transformations: Option<Vec<StageLoadSelectItemKind>>,
        from_query: Option<Box<Query>>,
        files: Option<Vec<String>>,
        pattern: Option<String>,
        file_format: KeyValueOptions,
        copy_options: KeyValueOptions,
        validation_mode: Option<String>,
        partition: Option<Box<Expr>>,
    },
    /// ```sql
    /// OPEN cursor_name
    /// ```
    /// Opens a cursor.
    Open(OpenStatement),
    /// ```sql
    /// CLOSE
    /// ```
    /// Closes the portal underlying an open cursor.
    Close {
        /// Cursor name
        cursor: CloseCursor,
    },
    /// ```sql
    /// UPDATE
    /// ```
    Update(Update),
    /// ```sql
    /// DELETE
    /// ```
    Delete(Delete),
    /// ```sql
    /// CREATE VIEW
    /// ```
    CreateView(CreateView),
    /// ```sql
    /// CREATE TABLE
    /// ```
    CreateTable(CreateTable),
    /// ```sql
    /// CREATE VIRTUAL TABLE .. USING <module_name> (<module_args>)`
    /// ```
    /// Sqlite specific statement
    CreateVirtualTable {
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        name: ObjectName,
        if_not_exists: bool,
        module_name: Ident,
        module_args: Vec<Ident>,
    },
    /// ```sql
    /// `CREATE INDEX`
    /// ```
    CreateIndex(CreateIndex),
    /// ```sql
    /// CREATE ROLE
    /// ```
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createrole.html)
    CreateRole(CreateRole),
    /// ```sql
    /// CREATE SECRET
    /// ```
    /// See [DuckDB](https://duckdb.org/docs/sql/statements/create_secret.html)
    CreateSecret {
        or_replace: bool,
        temporary: Option<bool>,
        if_not_exists: bool,
        name: Option<Ident>,
        storage_specifier: Option<Ident>,
        secret_type: Ident,
        options: Vec<SecretOption>,
    },
    /// A `CREATE SERVER` statement.
    CreateServer(CreateServerStatement),
    /// ```sql
    /// CREATE POLICY
    /// ```
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createpolicy.html)
    CreatePolicy {
        name: Ident,
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
        policy_type: Option<CreatePolicyType>,
        command: Option<CreatePolicyCommand>,
        to: Option<Vec<Owner>>,
        using: Option<Expr>,
        with_check: Option<Expr>,
    },
    /// ```sql
    /// CREATE CONNECTOR
    /// ```
    /// See [Hive](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362034#LanguageManualDDL-CreateDataConnectorCreateConnector)
    CreateConnector(CreateConnector),
    /// ```sql
    /// CREATE OPERATOR
    /// ```
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createoperator.html)
    CreateOperator(CreateOperator),
    /// ```sql
    /// CREATE OPERATOR FAMILY
    /// ```
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createopfamily.html)
    CreateOperatorFamily(CreateOperatorFamily),
    /// ```sql
    /// CREATE OPERATOR CLASS
    /// ```
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-createopclass.html)
    CreateOperatorClass(CreateOperatorClass),
    /// ```sql
    /// ALTER TABLE
    /// ```
    AlterTable(AlterTable),
    /// ```sql
    /// ALTER SCHEMA
    /// ```
    /// See [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_schema_collate_statement)
    AlterSchema(AlterSchema),
    /// ```sql
    /// ALTER INDEX
    /// ```
    AlterIndex {
        name: ObjectName,
        operation: AlterIndexOperation,
    },
    /// ```sql
    /// ALTER VIEW
    /// ```
    AlterView {
        /// View name
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        name: ObjectName,
        columns: Vec<Ident>,
        query: Box<Query>,
        with_options: Vec<SqlOption>,
    },
    /// ```sql
    /// ALTER TYPE
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-altertype.html)
    /// ```
    AlterType(AlterType),
    /// ```sql
    /// ALTER OPERATOR
    /// ```
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-alteroperator.html)
    AlterOperator(AlterOperator),
    /// ```sql
    /// ALTER OPERATOR FAMILY
    /// ```
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-alteropfamily.html)
    AlterOperatorFamily(AlterOperatorFamily),
    /// ```sql
    /// ALTER ROLE
    /// ```
    AlterRole {
        name: Ident,
        operation: AlterRoleOperation,
    },
    /// ```sql
    /// ALTER POLICY <NAME> ON <TABLE NAME> [<OPERATION>]
    /// ```
    /// (Postgresql-specific)
    AlterPolicy {
        name: Ident,
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
        operation: AlterPolicyOperation,
    },
    /// ```sql
    /// ALTER CONNECTOR connector_name SET DCPROPERTIES(property_name=property_value, ...);
    /// or
    /// ALTER CONNECTOR connector_name SET URL new_url;
    /// or
    /// ALTER CONNECTOR connector_name SET OWNER [USER|ROLE] user_or_role;
    /// ```
    /// (Hive-specific)
    AlterConnector {
        name: Ident,
        properties: Option<Vec<SqlOption>>,
        url: Option<String>,
        owner: Option<ddl::AlterConnectorOwner>,
    },
    /// ```sql
    /// ALTER SESSION SET sessionParam
    /// ALTER SESSION UNSET <param_name> [ , <param_name> , ... ]
    /// ```
    /// See <https://docs.snowflake.com/en/sql-reference/sql/alter-session>
    AlterSession {
        /// true is to set for the session parameters, false is to unset
        set: bool,
        /// The session parameters to set or unset
        session_params: KeyValueOptions,
    },
    /// ```sql
    /// ATTACH DATABASE 'path/to/file' AS alias
    /// ```
    /// (SQLite-specific)
    AttachDatabase {
        /// The name to bind to the newly attached database
        schema_name: Ident,
        /// An expression that indicates the path to the database file
        database_file_name: Expr,
        /// true if the syntax is 'ATTACH DATABASE', false if it's just 'ATTACH'
        database: bool,
    },
    /// (DuckDB-specific)
    /// ```sql
    /// ATTACH 'sqlite_file.db' AS sqlite_db (READ_ONLY, TYPE SQLITE);
    /// ```
    /// See <https://duckdb.org/docs/sql/statements/attach.html>
    AttachDuckDBDatabase {
        if_not_exists: bool,
        /// true if the syntax is 'ATTACH DATABASE', false if it's just 'ATTACH'
        database: bool,
        /// An expression that indicates the path to the database file
        database_path: Ident,
        database_alias: Option<Ident>,
        attach_options: Vec<AttachDuckDBDatabaseOption>,
    },
    /// (DuckDB-specific)
    /// ```sql
    /// DETACH db_alias;
    /// ```
    /// See <https://duckdb.org/docs/sql/statements/attach.html>
    DetachDuckDBDatabase {
        if_exists: bool,
        /// true if the syntax is 'DETACH DATABASE', false if it's just 'DETACH'
        database: bool,
        database_alias: Ident,
    },
    /// ```sql
    /// DROP [TABLE, VIEW, ...]
    /// ```
    Drop {
        /// The type of the object to drop: TABLE, VIEW, etc.
        object_type: ObjectType,
        /// An optional `IF EXISTS` clause. (Non-standard.)
        if_exists: bool,
        /// One or more objects to drop. (ANSI SQL requires exactly one.)
        names: Vec<ObjectName>,
        /// Whether `CASCADE` was specified. This will be `false` when
        /// `RESTRICT` or no drop behavior at all was specified.
        cascade: bool,
        /// Whether `RESTRICT` was specified. This will be `false` when
        /// `CASCADE` or no drop behavior at all was specified.
        restrict: bool,
        /// Hive allows you specify whether the table's stored data will be
        /// deleted along with the dropped table
        purge: bool,
        /// MySQL-specific "TEMPORARY" keyword
        temporary: bool,
        /// MySQL-specific drop index syntax, which requires table specification
        /// See <https://dev.mysql.com/doc/refman/8.4/en/drop-index.html>
        table: Option<ObjectName>,
    },
    /// ```sql
    /// DROP FUNCTION
    /// ```
    DropFunction(DropFunction),
    /// ```sql
    /// DROP DOMAIN
    /// ```
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-dropdomain.html)
    ///
    /// DROP DOMAIN [ IF EXISTS ] name [, ...] [ CASCADE | RESTRICT ]
    ///
    DropDomain(DropDomain),
    /// ```sql
    /// DROP PROCEDURE
    /// ```
    DropProcedure {
        if_exists: bool,
        /// One or more function to drop
        proc_desc: Vec<FunctionDesc>,
        /// `CASCADE` or `RESTRICT`
        drop_behavior: Option<DropBehavior>,
    },
    /// ```sql
    /// DROP SECRET
    /// ```
    DropSecret {
        if_exists: bool,
        temporary: Option<bool>,
        name: Ident,
        storage_specifier: Option<Ident>,
    },
    ///```sql
    /// DROP POLICY
    /// ```
    /// See [PostgreSQL](https://www.postgresql.org/docs/current/sql-droppolicy.html)
    DropPolicy {
        if_exists: bool,
        name: Ident,
        table_name: ObjectName,
        drop_behavior: Option<DropBehavior>,
    },
    /// ```sql
    /// DROP CONNECTOR
    /// ```
    /// See [Hive](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362034#LanguageManualDDL-DropConnector)
    DropConnector {
        if_exists: bool,
        name: Ident,
    },
    /// ```sql
    /// DECLARE
    /// ```
    /// Declare Cursor Variables
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    Declare {
        stmts: Vec<Declare>,
    },
    /// ```sql
    /// CREATE EXTENSION [ IF NOT EXISTS ] extension_name
    ///     [ WITH ] [ SCHEMA schema_name ]
    ///              [ VERSION version ]
    ///              [ CASCADE ]
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statement,
    CreateExtension(CreateExtension),
    /// ```sql
    /// DROP EXTENSION [ IF EXISTS ] name [, ...] [ CASCADE | RESTRICT ]
    /// ```
    /// Note: this is a PostgreSQL-specific statement.
    /// <https://www.postgresql.org/docs/current/sql-dropextension.html>
    DropExtension(DropExtension),
    /// ```sql
    /// DROP OPERATOR [ IF EXISTS ] name ( { left_type | NONE } , right_type ) [, ...] [ CASCADE | RESTRICT ]
    /// ```
    /// Note: this is a PostgreSQL-specific statement.
    /// <https://www.postgresql.org/docs/current/sql-dropoperator.html>
    DropOperator(DropOperator),
    /// ```sql
    /// DROP OPERATOR FAMILY [ IF EXISTS ] name USING index_method [ CASCADE | RESTRICT ]
    /// ```
    /// Note: this is a PostgreSQL-specific statement.
    /// <https://www.postgresql.org/docs/current/sql-dropopfamily.html>
    DropOperatorFamily(DropOperatorFamily),
    /// ```sql
    /// DROP OPERATOR CLASS [ IF EXISTS ] name USING index_method [ CASCADE | RESTRICT ]
    /// ```
    /// Note: this is a PostgreSQL-specific statement.
    /// <https://www.postgresql.org/docs/current/sql-dropopclass.html>
    DropOperatorClass(DropOperatorClass),
    /// ```sql
    /// FETCH
    /// ```
    /// Retrieve rows from a query using a cursor
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    Fetch {
        /// Cursor name
        name: Ident,
        direction: FetchDirection,
        position: FetchPosition,
        /// Optional, It's possible to fetch rows form cursor to the table
        into: Option<ObjectName>,
    },
    /// ```sql
    /// FLUSH [NO_WRITE_TO_BINLOG | LOCAL] flush_option [, flush_option] ... | tables_option
    /// ```
    ///
    /// Note: this is a Mysql-specific statement,
    /// but may also compatible with other SQL.
    Flush {
        object_type: FlushType,
        location: Option<FlushLocation>,
        channel: Option<String>,
        read_lock: bool,
        export: bool,
        tables: Vec<ObjectName>,
    },
    /// ```sql
    /// DISCARD [ ALL | PLANS | SEQUENCES | TEMPORARY | TEMP ]
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    Discard {
        object_type: DiscardObject,
    },
    /// `SHOW FUNCTIONS`
    ///
    /// Note: this is a Presto-specific statement.
    ShowFunctions {
        filter: Option<ShowStatementFilter>,
    },
    /// ```sql
    /// SHOW <variable>
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statement.
    ShowVariable {
        variable: Vec<Ident>,
    },
    /// ```sql
    /// SHOW [GLOBAL | SESSION] STATUS [LIKE 'pattern' | WHERE expr]
    /// ```
    ///
    /// Note: this is a MySQL-specific statement.
    ShowStatus {
        filter: Option<ShowStatementFilter>,
        global: bool,
        session: bool,
    },
    /// ```sql
    /// SHOW VARIABLES
    /// ```
    ///
    /// Note: this is a MySQL-specific statement.
    ShowVariables {
        filter: Option<ShowStatementFilter>,
        global: bool,
        session: bool,
    },
    /// ```sql
    /// SHOW CREATE TABLE
    /// ```
    ///
    /// Note: this is a MySQL-specific statement.
    ShowCreate {
        obj_type: ShowCreateObject,
        obj_name: ObjectName,
    },
    /// ```sql
    /// SHOW COLUMNS
    /// ```
    ShowColumns {
        extended: bool,
        full: bool,
        show_options: ShowStatementOptions,
    },
    /// ```sql
    /// SHOW DATABASES
    /// ```
    ShowDatabases {
        terse: bool,
        history: bool,
        show_options: ShowStatementOptions,
    },
    /// ```sql
    /// SHOW SCHEMAS
    /// ```
    ShowSchemas {
        terse: bool,
        history: bool,
        show_options: ShowStatementOptions,
    },
    // ```sql
    // SHOW {CHARACTER SET | CHARSET}
    // ```
    // [MySQL]:
    // <https://dev.mysql.com/doc/refman/8.4/en/show.html#:~:text=SHOW%20%7BCHARACTER%20SET%20%7C%20CHARSET%7D%20%5Blike_or_where%5D>
    ShowCharset(ShowCharset),
    /// ```sql
    /// SHOW OBJECTS LIKE 'line%' IN mydb.public
    /// ```
    /// Snowflake-specific statement
    /// <https://docs.snowflake.com/en/sql-reference/sql/show-objects>
    ShowObjects(ShowObjects),
    /// ```sql
    /// SHOW TABLES
    /// ```
    ShowTables {
        terse: bool,
        history: bool,
        extended: bool,
        full: bool,
        external: bool,
        show_options: ShowStatementOptions,
    },
    /// ```sql
    /// SHOW VIEWS
    /// ```
    ShowViews {
        terse: bool,
        materialized: bool,
        show_options: ShowStatementOptions,
    },
    /// ```sql
    /// SHOW COLLATION
    /// ```
    ///
    /// Note: this is a MySQL-specific statement.
    ShowCollation {
        filter: Option<ShowStatementFilter>,
    },
    /// ```sql
    /// `USE ...`
    /// ```
    Use(Use),
    /// ```sql
    /// START  [ TRANSACTION | WORK ] | START TRANSACTION } ...
    /// ```
    /// If `begin` is false.
    ///
    /// ```sql
    /// `BEGIN  [ TRANSACTION | WORK ] | START TRANSACTION } ...`
    /// ```
    /// If `begin` is true
    StartTransaction {
        modes: Vec<TransactionMode>,
        begin: bool,
        transaction: Option<BeginTransactionKind>,
        modifier: Option<TransactionModifier>,
        /// List of statements belonging to the `BEGIN` block.
        /// Example:
        /// ```sql
        /// BEGIN
        ///     SELECT 1;
        ///     SELECT 2;
        /// END;
        /// ```
        statements: Vec<Statement>,
        /// Exception handling with exception clauses.
        /// Example:
        /// ```sql
        /// EXCEPTION
        ///     WHEN EXCEPTION_1 THEN
        ///         SELECT 2;
        ///     WHEN EXCEPTION_2 OR EXCEPTION_3 THEN
        ///         SELECT 3;
        ///     WHEN OTHER THEN
        ///         SELECT 4;
        /// ```
        /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#beginexceptionend>
        /// <https://docs.snowflake.com/en/sql-reference/snowflake-scripting/exception>
        exception: Option<Vec<ExceptionWhen>>,
        /// TRUE if the statement has an `END` keyword.
        has_end_keyword: bool,
    },
    /// ```sql
    /// COMMENT ON ...
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Comment {
        object_type: CommentObject,
        object_name: ObjectName,
        comment: Option<String>,
        /// An optional `IF EXISTS` clause. (Non-standard.)
        /// See <https://docs.snowflake.com/en/sql-reference/sql/comment>
        if_exists: bool,
    },
    /// ```sql
    /// COMMIT [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]
    /// ```
    /// If `end` is false
    ///
    /// ```sql
    /// END [ TRY | CATCH ]
    /// ```
    /// If `end` is true
    Commit {
        chain: bool,
        end: bool,
        modifier: Option<TransactionModifier>,
    },
    /// ```sql
    /// ROLLBACK [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ] [ TO [ SAVEPOINT ] savepoint_name ]
    /// ```
    Rollback {
        chain: bool,
        savepoint: Option<Ident>,
    },
    /// ```sql
    /// CREATE SCHEMA
    /// ```
    CreateSchema {
        /// `<schema name> | AUTHORIZATION <schema authorization identifier>  | <schema name>  AUTHORIZATION <schema authorization identifier>`
        schema_name: SchemaName,
        if_not_exists: bool,
        /// Schema properties.
        ///
        /// ```sql
        /// CREATE SCHEMA myschema WITH (key1='value1');
        /// ```
        ///
        /// [Trino](https://trino.io/docs/current/sql/create-schema.html)
        with: Option<Vec<SqlOption>>,
        /// Schema options.
        ///
        /// ```sql
        /// CREATE SCHEMA myschema OPTIONS(key1='value1');
        /// ```
        ///
        /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement)
        options: Option<Vec<SqlOption>>,
        /// Default collation specification for the schema.
        ///
        /// ```sql
        /// CREATE SCHEMA myschema DEFAULT COLLATE 'und:ci';
        /// ```
        ///
        /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement)
        default_collate_spec: Option<Expr>,
        /// Clones a schema
        ///
        /// ```sql
        /// CREATE SCHEMA myschema CLONE otherschema
        /// ```
        ///
        /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-clone#databases-schemas)
        clone: Option<ObjectName>,
    },
    /// ```sql
    /// CREATE DATABASE
    /// ```
    /// See:
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-database>
    CreateDatabase {
        db_name: ObjectName,
        if_not_exists: bool,
        location: Option<String>,
        managed_location: Option<String>,
        or_replace: bool,
        transient: bool,
        clone: Option<ObjectName>,
        data_retention_time_in_days: Option<u64>,
        max_data_extension_time_in_days: Option<u64>,
        external_volume: Option<String>,
        catalog: Option<String>,
        replace_invalid_characters: Option<bool>,
        default_ddl_collation: Option<String>,
        storage_serialization_policy: Option<StorageSerializationPolicy>,
        comment: Option<String>,
        catalog_sync: Option<String>,
        catalog_sync_namespace_mode: Option<CatalogSyncNamespaceMode>,
        catalog_sync_namespace_flatten_delimiter: Option<String>,
        with_tags: Option<Vec<Tag>>,
        with_contacts: Option<Vec<ContactEntry>>,
    },
    /// ```sql
    /// CREATE FUNCTION
    /// ```
    ///
    /// Supported variants:
    /// 1. [Hive](https://cwiki.apache.org/confluence/display/hive/languagemanual+ddl#LanguageManualDDL-Create/Drop/ReloadFunction)
    /// 2. [PostgreSQL](https://www.postgresql.org/docs/15/sql-createfunction.html)
    /// 3. [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement)
    /// 4. [MsSql](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql)
    CreateFunction(CreateFunction),
    /// CREATE TRIGGER statement. See struct [CreateTrigger] for details.
    CreateTrigger(CreateTrigger),
    /// DROP TRIGGER statement. See struct [DropTrigger] for details.
    DropTrigger(DropTrigger),
    /// ```sql
    /// CREATE PROCEDURE
    /// ```
    CreateProcedure {
        or_alter: bool,
        name: ObjectName,
        params: Option<Vec<ProcedureParam>>,
        language: Option<Ident>,
        body: ConditionalStatements,
    },
    /// ```sql
    /// CREATE MACRO
    /// ```
    ///
    /// Supported variants:
    /// 1. [DuckDB](https://duckdb.org/docs/sql/statements/create_macro)
    CreateMacro {
        or_replace: bool,
        temporary: bool,
        name: ObjectName,
        args: Option<Vec<MacroArg>>,
        definition: MacroDefinition,
    },
    /// ```sql
    /// CREATE STAGE
    /// ```
    /// See <https://docs.snowflake.com/en/sql-reference/sql/create-stage>
    CreateStage {
        or_replace: bool,
        temporary: bool,
        if_not_exists: bool,
        name: ObjectName,
        stage_params: StageParamsObject,
        directory_table_params: KeyValueOptions,
        file_format: KeyValueOptions,
        copy_options: KeyValueOptions,
        comment: Option<String>,
    },
    /// ```sql
    /// ASSERT <condition> [AS <message>]
    /// ```
    Assert {
        condition: Expr,
        message: Option<Expr>,
    },
    /// ```sql
    /// GRANT privileges ON objects TO grantees
    /// ```
    Grant {
        privileges: Privileges,
        objects: Option<GrantObjects>,
        grantees: Vec<Grantee>,
        with_grant_option: bool,
        as_grantor: Option<Ident>,
        granted_by: Option<Ident>,
        current_grants: Option<CurrentGrantsKind>,
    },
    /// ```sql
    /// DENY privileges ON object TO grantees
    /// ```
    Deny(DenyStatement),
    /// ```sql
    /// REVOKE privileges ON objects FROM grantees
    /// ```
    Revoke {
        privileges: Privileges,
        objects: Option<GrantObjects>,
        grantees: Vec<Grantee>,
        granted_by: Option<Ident>,
        cascade: Option<CascadeOption>,
    },
    /// ```sql
    /// DEALLOCATE [ PREPARE ] { name | ALL }
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Deallocate {
        name: Ident,
        prepare: bool,
    },
    /// ```sql
    /// An `EXECUTE` statement
    /// ```
    ///
    /// Postgres: <https://www.postgresql.org/docs/current/sql-execute.html>
    /// MSSQL: <https://learn.microsoft.com/en-us/sql/relational-databases/stored-procedures/execute-a-stored-procedure>
    /// BigQuery: <https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#execute_immediate>
    /// Snowflake: <https://docs.snowflake.com/en/sql-reference/sql/execute-immediate>
    Execute {
        name: Option<ObjectName>,
        parameters: Vec<Expr>,
        has_parentheses: bool,
        /// Is this an `EXECUTE IMMEDIATE`
        immediate: bool,
        into: Vec<Ident>,
        using: Vec<ExprWithAlias>,
        /// Whether the last parameter is the return value of the procedure
        /// MSSQL: <https://learn.microsoft.com/en-us/sql/t-sql/language-elements/execute-transact-sql?view=sql-server-ver17#output>
        output: bool,
        /// Whether to invoke the procedure with the default parameter values
        /// MSSQL: <https://learn.microsoft.com/en-us/sql/t-sql/language-elements/execute-transact-sql?view=sql-server-ver17#default>
        default: bool,
    },
    /// ```sql
    /// PREPARE name [ ( data_type [, ...] ) ] AS statement
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Prepare {
        name: Ident,
        data_types: Vec<DataType>,
        statement: Box<Statement>,
    },
    /// ```sql
    /// KILL [CONNECTION | QUERY | MUTATION]
    /// ```
    ///
    /// See <https://clickhouse.com/docs/en/sql-reference/statements/kill/>
    /// See <https://dev.mysql.com/doc/refman/8.0/en/kill.html>
    Kill {
        modifier: Option<KillType>,
        // processlist_id
        id: u64,
    },
    /// ```sql
    /// [EXPLAIN | DESC | DESCRIBE] TABLE
    /// ```
    /// Note: this is a MySQL-specific statement. See <https://dev.mysql.com/doc/refman/8.0/en/explain.html>
    ExplainTable {
        /// `EXPLAIN | DESC | DESCRIBE`
        describe_alias: DescribeAlias,
        /// Hive style `FORMATTED | EXTENDED`
        hive_format: Option<HiveDescribeFormat>,
        /// Snowflake and ClickHouse support `DESC|DESCRIBE TABLE <table_name>` syntax
        ///
        /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/desc-table.html)
        /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/describe-table)
        has_table_keyword: bool,
        /// Table name
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
    },
    /// ```sql
    /// [EXPLAIN | DESC | DESCRIBE]  <statement>
    /// ```
    Explain {
        /// `EXPLAIN | DESC | DESCRIBE`
        describe_alias: DescribeAlias,
        /// Carry out the command and show actual run times and other statistics.
        analyze: bool,
        // Display additional information regarding the plan.
        verbose: bool,
        /// `EXPLAIN QUERY PLAN`
        /// Display the query plan without running the query.
        ///
        /// [SQLite](https://sqlite.org/lang_explain.html)
        query_plan: bool,
        /// `EXPLAIN ESTIMATE`
        /// [Clickhouse](https://clickhouse.com/docs/en/sql-reference/statements/explain#explain-estimate)
        estimate: bool,
        /// A SQL query that specifies what to explain
        statement: Box<Statement>,
        /// Optional output format of explain
        format: Option<AnalyzeFormatKind>,
        /// Postgres style utility options, `(analyze, verbose true)`
        options: Option<Vec<UtilityOption>>,
    },
    /// ```sql
    /// SAVEPOINT
    /// ```
    /// Define a new savepoint within the current transaction
    Savepoint {
        name: Ident,
    },
    /// ```sql
    /// RELEASE [ SAVEPOINT ] savepoint_name
    /// ```
    ReleaseSavepoint {
        name: Ident,
    },
    /// A `MERGE` statement.
    ///
    /// ```sql
    /// MERGE INTO <target_table> USING <source> ON <join_expr> { matchedClause | notMatchedClause } [ ... ]
    /// ```
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/merge)
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement)
    /// [MSSQL](https://learn.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql?view=sql-server-ver16)
    Merge(Merge),
    /// ```sql
    /// CACHE [ FLAG ] TABLE <table_name> [ OPTIONS('K1' = 'V1', 'K2' = V2) ] [ AS ] [ <query> ]
    /// ```
    ///
    /// See [Spark SQL docs] for more details.
    ///
    /// [Spark SQL docs]: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-aux-cache-cache-table.html
    Cache {
        /// Table flag
        table_flag: Option<ObjectName>,
        /// Table name
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
        has_as: bool,
        /// Table confs
        options: Vec<SqlOption>,
        /// Cache table as a Query
        query: Option<Box<Query>>,
    },
    /// ```sql
    /// UNCACHE TABLE [ IF EXISTS ]  <table_name>
    /// ```
    UNCache {
        /// Table name
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
        if_exists: bool,
    },
    /// ```sql
    /// CREATE [ { TEMPORARY | TEMP } ] SEQUENCE [ IF NOT EXISTS ] <sequence_name>
    /// ```
    /// Define a new sequence:
    CreateSequence {
        temporary: bool,
        if_not_exists: bool,
        name: ObjectName,
        data_type: Option<DataType>,
        sequence_options: Vec<SequenceOptions>,
        owned_by: Option<ObjectName>,
    },
    /// A `CREATE DOMAIN` statement.
    CreateDomain(CreateDomain),
    /// ```sql
    /// CREATE TYPE <name>
    /// ```
    CreateType {
        name: ObjectName,
        representation: Option<UserDefinedTypeRepresentation>,
    },
    /// ```sql
    /// PRAGMA <schema-name>.<pragma-name> = <pragma-value>
    /// ```
    Pragma {
        name: ObjectName,
        value: Option<Value>,
        is_eq: bool,
    },
    /// ```sql
    /// LOCK TABLES <table_name> [READ [LOCAL] | [LOW_PRIORITY] WRITE]
    /// ```
    /// Note: this is a MySQL-specific statement. See <https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html>
    LockTables {
        tables: Vec<LockTable>,
    },
    /// ```sql
    /// UNLOCK TABLES
    /// ```
    /// Note: this is a MySQL-specific statement. See <https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html>
    UnlockTables,
    /// Unloads the result of a query to file
    ///
    /// [Athena](https://docs.aws.amazon.com/athena/latest/ug/unload.html):
    /// ```sql
    /// UNLOAD(statement) TO <destination> [ WITH options ]
    /// ```
    ///
    /// [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html):
    /// ```sql
    /// UNLOAD('statement') TO <destination> [ OPTIONS ]
    /// ```
    Unload {
        query: Option<Box<Query>>,
        query_text: Option<String>,
        to: Ident,
        auth: Option<IamRoleKind>,
        with: Vec<SqlOption>,
        options: Vec<CopyLegacyOption>,
    },
    /// ```sql
    /// OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE [BY expression]]
    /// ```
    ///
    /// See ClickHouse <https://clickhouse.com/docs/en/sql-reference/statements/optimize>
    OptimizeTable {
        name: ObjectName,
        on_cluster: Option<Ident>,
        partition: Option<Partition>,
        include_final: bool,
        deduplicate: Option<Deduplicate>,
    },
    /// ```sql
    /// LISTEN
    /// ```
    /// listen for a notification channel
    ///
    /// See Postgres <https://www.postgresql.org/docs/current/sql-listen.html>
    LISTEN {
        channel: Ident,
    },
    /// ```sql
    /// UNLISTEN
    /// ```
    /// stop listening for a notification
    ///
    /// See Postgres <https://www.postgresql.org/docs/current/sql-unlisten.html>
    UNLISTEN {
        channel: Ident,
    },
    /// ```sql
    /// NOTIFY channel [ , payload ]
    /// ```
    /// send a notification event together with an optional "payload" string to channel
    ///
    /// See Postgres <https://www.postgresql.org/docs/current/sql-notify.html>
    NOTIFY {
        channel: Ident,
        payload: Option<String>,
    },
    /// ```sql
    /// LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
    /// [PARTITION (partcol1=val1, partcol2=val2 ...)]
    /// [INPUTFORMAT 'inputformat' SERDE 'serde']
    /// ```
    /// Loading files into tables
    ///
    /// See Hive <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27362036#LanguageManualDML-Loadingfilesintotables>
    LoadData {
        local: bool,
        inpath: String,
        overwrite: bool,
        table_name: ObjectName,
        partitioned: Option<Vec<Expr>>,
        table_format: Option<HiveLoadDataFormat>,
    },
    /// ```sql
    /// Rename TABLE tbl_name TO new_tbl_name[, tbl_name2 TO new_tbl_name2] ...
    /// ```
    /// Renames one or more tables
    ///
    /// See Mysql <https://dev.mysql.com/doc/refman/9.1/en/rename-table.html>
    RenameTable(Vec<RenameTable>),
    /// Snowflake `LIST`
    /// See: <https://docs.snowflake.com/en/sql-reference/sql/list>
    List(FileStagingCommand),
    /// Snowflake `REMOVE`
    /// See: <https://docs.snowflake.com/en/sql-reference/sql/remove>
    Remove(FileStagingCommand),
    /// RaiseError (MSSQL)
    /// RAISERROR ( { msg_id | msg_str | @local_variable }
    /// { , severity , state }
    /// [ , argument [ , ...n ] ] )
    /// [ WITH option [ , ...n ] ]
    /// See <https://learn.microsoft.com/en-us/sql/t-sql/language-elements/raiserror-transact-sql?view=sql-server-ver16>
    RaisError {
        message: Box<Expr>,
        severity: Box<Expr>,
        state: Box<Expr>,
        arguments: Vec<Expr>,
        options: Vec<RaisErrorOption>,
    },
    /// ```sql
    /// PRINT msg_str | @local_variable | string_expr
    /// ```
    ///
    /// See: <https://learn.microsoft.com/en-us/sql/t-sql/statements/print-transact-sql>
    Print(PrintStatement),
    /// ```sql
    /// RETURN [ expression ]
    /// ```
    ///
    /// See [ReturnStatement]
    Return(ReturnStatement),
    /// Export data statement
    ///
    /// Example:
    /// ```sql
    /// EXPORT DATA OPTIONS(uri='gs://bucket/folder/*', format='PARQUET', overwrite=true) AS
    /// SELECT field1, field2 FROM mydataset.table1 ORDER BY field1 LIMIT 10
    /// ```
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/export-statements)
    ExportData(ExportData),
    /// ```sql
    /// CREATE [OR REPLACE] USER <user> [IF NOT EXISTS]
    /// ```
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-user)
    CreateUser(CreateUser),
    /// ```sql
    /// ALTER USER \[ IF EXISTS \] \[ <name> \]
    /// ```
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/alter-user)
    AlterUser(AlterUser),
    /// Re-sorts rows and reclaims space in either a specified table or all tables in the current database
    ///
    /// ```sql
    /// VACUUM tbl
    /// ```
    /// [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/r_VACUUM_command.html)
    Vacuum(VacuumStatement),
    /// Restore the value of a run-time parameter to the default value.
    ///
    /// ```sql
    /// RESET configuration_parameter;
    /// RESET ALL;
    /// ```
    /// [PostgreSQL](https://www.postgresql.org/docs/current/sql-reset.html)
    Reset(ResetStatement),
}

impl From<Analyze> for Statement {
    fn from(analyze: Analyze) -> Self {
        Statement::Analyze(analyze)
    }
}

impl From<ddl::Truncate> for Statement {
    fn from(truncate: ddl::Truncate) -> Self {
        Statement::Truncate(truncate)
    }
}

impl From<ddl::Msck> for Statement {
    fn from(msck: ddl::Msck) -> Self {
        Statement::Msck(msck)
    }
}

/// ```sql
/// {COPY | REVOKE} CURRENT GRANTS
/// ```
///
/// - [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/grant-ownership#optional-parameters)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CurrentGrantsKind {
    CopyCurrentGrants,
    RevokeCurrentGrants,
}

impl fmt::Display for CurrentGrantsKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CurrentGrantsKind::CopyCurrentGrants => write!(f, "COPY CURRENT GRANTS"),
            CurrentGrantsKind::RevokeCurrentGrants => write!(f, "REVOKE CURRENT GRANTS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RaisErrorOption {
    Log,
    NoWait,
    SetError,
}

impl fmt::Display for RaisErrorOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RaisErrorOption::Log => write!(f, "LOG"),
            RaisErrorOption::NoWait => write!(f, "NOWAIT"),
            RaisErrorOption::SetError => write!(f, "SETERROR"),
        }
    }
}

impl fmt::Display for Statement {
    /// Formats a SQL statement with support for pretty printing.
    ///
    /// When using the alternate flag (`{:#}`), the statement will be formatted with proper
    /// indentation and line breaks. For example:
    ///
    /// ```
    /// # use sqlparser::dialect::GenericDialect;
    /// # use sqlparser::parser::Parser;
    /// let sql = "SELECT a, b FROM table_1";
    /// let ast = Parser::parse_sql(&GenericDialect, sql).unwrap();
    ///
    /// // Regular formatting
    /// assert_eq!(format!("{}", ast[0]), "SELECT a, b FROM table_1");
    ///
    /// // Pretty printing
    /// assert_eq!(format!("{:#}", ast[0]),
    /// r#"SELECT
    ///   a,
    ///   b
    /// FROM
    ///   table_1"#);
    /// ```
    // Clippy thinks this function is too complicated, but it is painful to
    // split up without extracting structs for each `Statement` variant.
    #[allow(clippy::cognitive_complexity)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::Flush {
                object_type,
                location,
                channel,
                read_lock,
                export,
                tables,
            } => {
                write!(f, "FLUSH")?;
                if let Some(location) = location {
                    f.write_str(" ")?;
                    location.fmt(f)?;
                }
                write!(f, " {object_type}")?;

                if let Some(channel) = channel {
                    write!(f, " FOR CHANNEL {channel}")?;
                }

                write!(
                    f,
                    "{tables}{read}{export}",
                    tables = if !tables.is_empty() {
                        " ".to_string() + &display_comma_separated(tables).to_string()
                    } else {
                        "".to_string()
                    },
                    export = if *export { " FOR EXPORT" } else { "" },
                    read = if *read_lock { " WITH READ LOCK" } else { "" }
                )
            }
            Statement::Kill { modifier, id } => {
                write!(f, "KILL ")?;

                if let Some(m) = modifier {
                    write!(f, "{m} ")?;
                }

                write!(f, "{id}")
            }
            Statement::ExplainTable {
                describe_alias,
                hive_format,
                has_table_keyword,
                table_name,
            } => {
                write!(f, "{describe_alias} ")?;

                if let Some(format) = hive_format {
                    write!(f, "{format} ")?;
                }
                if *has_table_keyword {
                    write!(f, "TABLE ")?;
                }

                write!(f, "{table_name}")
            }
            Statement::Explain {
                describe_alias,
                verbose,
                analyze,
                query_plan,
                estimate,
                statement,
                format,
                options,
            } => {
                write!(f, "{describe_alias} ")?;

                if *query_plan {
                    write!(f, "QUERY PLAN ")?;
                }
                if *analyze {
                    write!(f, "ANALYZE ")?;
                }
                if *estimate {
                    write!(f, "ESTIMATE ")?;
                }

                if *verbose {
                    write!(f, "VERBOSE ")?;
                }

                if let Some(format) = format {
                    write!(f, "{format} ")?;
                }

                if let Some(options) = options {
                    write!(f, "({}) ", display_comma_separated(options))?;
                }

                write!(f, "{statement}")
            }
            Statement::Query(s) => s.fmt(f),
            Statement::Declare { stmts } => {
                write!(f, "DECLARE ")?;
                write!(f, "{}", display_separated(stmts, "; "))
            }
            Statement::Fetch {
                name,
                direction,
                position,
                into,
            } => {
                write!(f, "FETCH {direction} {position} {name}")?;

                if let Some(into) = into {
                    write!(f, " INTO {into}")?;
                }

                Ok(())
            }
            Statement::Directory {
                overwrite,
                local,
                path,
                file_format,
                source,
            } => {
                write!(
                    f,
                    "INSERT{overwrite}{local} DIRECTORY '{path}'",
                    overwrite = if *overwrite { " OVERWRITE" } else { "" },
                    local = if *local { " LOCAL" } else { "" },
                    path = path
                )?;
                if let Some(ref ff) = file_format {
                    write!(f, " STORED AS {ff}")?
                }
                write!(f, " {source}")
            }
            Statement::Msck(msck) => msck.fmt(f),
            Statement::Truncate(truncate) => truncate.fmt(f),
            Statement::Case(stmt) => {
                write!(f, "{stmt}")
            }
            Statement::If(stmt) => {
                write!(f, "{stmt}")
            }
            Statement::While(stmt) => {
                write!(f, "{stmt}")
            }
            Statement::Raise(stmt) => {
                write!(f, "{stmt}")
            }
            Statement::AttachDatabase {
                schema_name,
                database_file_name,
                database,
            } => {
                let keyword = if *database { "DATABASE " } else { "" };
                write!(f, "ATTACH {keyword}{database_file_name} AS {schema_name}")
            }
            Statement::AttachDuckDBDatabase {
                if_not_exists,
                database,
                database_path,
                database_alias,
                attach_options,
            } => {
                write!(
                    f,
                    "ATTACH{database}{if_not_exists} {database_path}",
                    database = if *database { " DATABASE" } else { "" },
                    if_not_exists = if *if_not_exists { " IF NOT EXISTS" } else { "" },
                )?;
                if let Some(alias) = database_alias {
                    write!(f, " AS {alias}")?;
                }
                if !attach_options.is_empty() {
                    write!(f, " ({})", display_comma_separated(attach_options))?;
                }
                Ok(())
            }
            Statement::DetachDuckDBDatabase {
                if_exists,
                database,
                database_alias,
            } => {
                write!(
                    f,
                    "DETACH{database}{if_exists} {database_alias}",
                    database = if *database { " DATABASE" } else { "" },
                    if_exists = if *if_exists { " IF EXISTS" } else { "" },
                )?;
                Ok(())
            }
            Statement::Analyze(analyze) => analyze.fmt(f),
            Statement::Insert(insert) => insert.fmt(f),
            Statement::Install {
                extension_name: name,
            } => write!(f, "INSTALL {name}"),

            Statement::Load {
                extension_name: name,
            } => write!(f, "LOAD {name}"),

            Statement::Call(function) => write!(f, "CALL {function}"),

            Statement::Copy {
                source,
                to,
                target,
                options,
                legacy_options,
                values,
            } => {
                write!(f, "COPY")?;
                match source {
                    CopySource::Query(query) => write!(f, " ({query})")?,
                    CopySource::Table {
                        table_name,
                        columns,
                    } => {
                        write!(f, " {table_name}")?;
                        if !columns.is_empty() {
                            write!(f, " ({})", display_comma_separated(columns))?;
                        }
                    }
                }
                write!(f, " {} {}", if *to { "TO" } else { "FROM" }, target)?;
                if !options.is_empty() {
                    write!(f, " ({})", display_comma_separated(options))?;
                }
                if !legacy_options.is_empty() {
                    write!(f, " {}", display_separated(legacy_options, " "))?;
                }
                if !values.is_empty() {
                    writeln!(f, ";")?;
                    let mut delim = "";
                    for v in values {
                        write!(f, "{delim}")?;
                        delim = "\t";
                        if let Some(v) = v {
                            write!(f, "{v}")?;
                        } else {
                            write!(f, "\\N")?;
                        }
                    }
                    write!(f, "\n\\.")?;
                }
                Ok(())
            }
            Statement::Update(update) => update.fmt(f),
            Statement::Delete(delete) => delete.fmt(f),
            Statement::Open(open) => open.fmt(f),
            Statement::Close { cursor } => {
                write!(f, "CLOSE {cursor}")?;

                Ok(())
            }
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                location,
                managed_location,
                or_replace,
                transient,
                clone,
                data_retention_time_in_days,
                max_data_extension_time_in_days,
                external_volume,
                catalog,
                replace_invalid_characters,
                default_ddl_collation,
                storage_serialization_policy,
                comment,
                catalog_sync,
                catalog_sync_namespace_mode,
                catalog_sync_namespace_flatten_delimiter,
                with_tags,
                with_contacts,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}{transient}DATABASE {if_not_exists}{name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    transient = if *transient { "TRANSIENT " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    name = db_name,
                )?;

                if let Some(l) = location {
                    write!(f, " LOCATION '{l}'")?;
                }
                if let Some(ml) = managed_location {
                    write!(f, " MANAGEDLOCATION '{ml}'")?;
                }
                if let Some(clone) = clone {
                    write!(f, " CLONE {clone}")?;
                }

                if let Some(value) = data_retention_time_in_days {
                    write!(f, " DATA_RETENTION_TIME_IN_DAYS = {value}")?;
                }

                if let Some(value) = max_data_extension_time_in_days {
                    write!(f, " MAX_DATA_EXTENSION_TIME_IN_DAYS = {value}")?;
                }

                if let Some(vol) = external_volume {
                    write!(f, " EXTERNAL_VOLUME = '{vol}'")?;
                }

                if let Some(cat) = catalog {
                    write!(f, " CATALOG = '{cat}'")?;
                }

                if let Some(true) = replace_invalid_characters {
                    write!(f, " REPLACE_INVALID_CHARACTERS = TRUE")?;
                } else if let Some(false) = replace_invalid_characters {
                    write!(f, " REPLACE_INVALID_CHARACTERS = FALSE")?;
                }

                if let Some(collation) = default_ddl_collation {
                    write!(f, " DEFAULT_DDL_COLLATION = '{collation}'")?;
                }

                if let Some(policy) = storage_serialization_policy {
                    write!(f, " STORAGE_SERIALIZATION_POLICY = {policy}")?;
                }

                if let Some(comment) = comment {
                    write!(f, " COMMENT = '{comment}'")?;
                }

                if let Some(sync) = catalog_sync {
                    write!(f, " CATALOG_SYNC = '{sync}'")?;
                }

                if let Some(mode) = catalog_sync_namespace_mode {
                    write!(f, " CATALOG_SYNC_NAMESPACE_MODE = {mode}")?;
                }

                if let Some(delim) = catalog_sync_namespace_flatten_delimiter {
                    write!(f, " CATALOG_SYNC_NAMESPACE_FLATTEN_DELIMITER = '{delim}'")?;
                }

                if let Some(tags) = with_tags {
                    write!(f, " WITH TAG ({})", display_comma_separated(tags))?;
                }

                if let Some(contacts) = with_contacts {
                    write!(f, " WITH CONTACT ({})", display_comma_separated(contacts))?;
                }
                Ok(())
            }
            Statement::CreateFunction(create_function) => create_function.fmt(f),
            Statement::CreateDomain(create_domain) => create_domain.fmt(f),
            Statement::CreateTrigger(create_trigger) => create_trigger.fmt(f),
            Statement::DropTrigger(drop_trigger) => drop_trigger.fmt(f),
            Statement::CreateProcedure {
                name,
                or_alter,
                params,
                language,
                body,
            } => {
                write!(
                    f,
                    "CREATE {or_alter}PROCEDURE {name}",
                    or_alter = if *or_alter { "OR ALTER " } else { "" },
                    name = name
                )?;

                if let Some(p) = params {
                    if !p.is_empty() {
                        write!(f, " ({})", display_comma_separated(p))?;
                    }
                }

                if let Some(language) = language {
                    write!(f, " LANGUAGE {language}")?;
                }

                write!(f, " AS {body}")
            }
            Statement::CreateMacro {
                or_replace,
                temporary,
                name,
                args,
                definition,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}{temp}MACRO {name}",
                    temp = if *temporary { "TEMPORARY " } else { "" },
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                )?;
                if let Some(args) = args {
                    write!(f, "({})", display_comma_separated(args))?;
                }
                match definition {
                    MacroDefinition::Expr(expr) => write!(f, " AS {expr}")?,
                    MacroDefinition::Table(query) => write!(f, " AS TABLE {query}")?,
                }
                Ok(())
            }
            Statement::CreateView(create_view) => create_view.fmt(f),
            Statement::CreateTable(create_table) => create_table.fmt(f),
            Statement::LoadData {
                local,
                inpath,
                overwrite,
                table_name,
                partitioned,
                table_format,
            } => {
                write!(
                    f,
                    "LOAD DATA {local}INPATH '{inpath}' {overwrite}INTO TABLE {table_name}",
                    local = if *local { "LOCAL " } else { "" },
                    inpath = inpath,
                    overwrite = if *overwrite { "OVERWRITE " } else { "" },
                    table_name = table_name,
                )?;
                if let Some(ref parts) = &partitioned {
                    if !parts.is_empty() {
                        write!(f, " PARTITION ({})", display_comma_separated(parts))?;
                    }
                }
                if let Some(HiveLoadDataFormat {
                    serde,
                    input_format,
                }) = &table_format
                {
                    write!(f, " INPUTFORMAT {input_format} SERDE {serde}")?;
                }
                Ok(())
            }
            Statement::CreateVirtualTable {
                name,
                if_not_exists,
                module_name,
                module_args,
            } => {
                write!(
                    f,
                    "CREATE VIRTUAL TABLE {if_not_exists}{name} USING {module_name}",
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    name = name,
                    module_name = module_name
                )?;
                if !module_args.is_empty() {
                    write!(f, " ({})", display_comma_separated(module_args))?;
                }
                Ok(())
            }
            Statement::CreateIndex(create_index) => create_index.fmt(f),
            Statement::CreateExtension(create_extension) => write!(f, "{create_extension}"),
            Statement::DropExtension(drop_extension) => write!(f, "{drop_extension}"),
            Statement::DropOperator(drop_operator) => write!(f, "{drop_operator}"),
            Statement::DropOperatorFamily(drop_operator_family) => {
                write!(f, "{drop_operator_family}")
            }
            Statement::DropOperatorClass(drop_operator_class) => {
                write!(f, "{drop_operator_class}")
            }
            Statement::CreateRole(create_role) => write!(f, "{create_role}"),
            Statement::CreateSecret {
                or_replace,
                temporary,
                if_not_exists,
                name,
                storage_specifier,
                secret_type,
                options,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                )?;
                if let Some(t) = temporary {
                    write!(f, "{}", if *t { "TEMPORARY " } else { "PERSISTENT " })?;
                }
                write!(
                    f,
                    "SECRET {if_not_exists}",
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                )?;
                if let Some(n) = name {
                    write!(f, "{n} ")?;
                };
                if let Some(s) = storage_specifier {
                    write!(f, "IN {s} ")?;
                }
                write!(f, "( TYPE {secret_type}",)?;
                if !options.is_empty() {
                    write!(f, ", {o}", o = display_comma_separated(options))?;
                }
                write!(f, " )")?;
                Ok(())
            }
            Statement::CreateServer(stmt) => {
                write!(f, "{stmt}")
            }
            Statement::CreatePolicy {
                name,
                table_name,
                policy_type,
                command,
                to,
                using,
                with_check,
            } => {
                write!(f, "CREATE POLICY {name} ON {table_name}")?;

                if let Some(policy_type) = policy_type {
                    match policy_type {
                        CreatePolicyType::Permissive => write!(f, " AS PERMISSIVE")?,
                        CreatePolicyType::Restrictive => write!(f, " AS RESTRICTIVE")?,
                    }
                }

                if let Some(command) = command {
                    match command {
                        CreatePolicyCommand::All => write!(f, " FOR ALL")?,
                        CreatePolicyCommand::Select => write!(f, " FOR SELECT")?,
                        CreatePolicyCommand::Insert => write!(f, " FOR INSERT")?,
                        CreatePolicyCommand::Update => write!(f, " FOR UPDATE")?,
                        CreatePolicyCommand::Delete => write!(f, " FOR DELETE")?,
                    }
                }

                if let Some(to) = to {
                    write!(f, " TO {}", display_comma_separated(to))?;
                }

                if let Some(using) = using {
                    write!(f, " USING ({using})")?;
                }

                if let Some(with_check) = with_check {
                    write!(f, " WITH CHECK ({with_check})")?;
                }

                Ok(())
            }
            Statement::CreateConnector(create_connector) => create_connector.fmt(f),
            Statement::CreateOperator(create_operator) => create_operator.fmt(f),
            Statement::CreateOperatorFamily(create_operator_family) => {
                create_operator_family.fmt(f)
            }
            Statement::CreateOperatorClass(create_operator_class) => create_operator_class.fmt(f),
            Statement::AlterTable(alter_table) => write!(f, "{alter_table}"),
            Statement::AlterIndex { name, operation } => {
                write!(f, "ALTER INDEX {name} {operation}")
            }
            Statement::AlterView {
                name,
                columns,
                query,
                with_options,
            } => {
                write!(f, "ALTER VIEW {name}")?;
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
                write!(f, " AS {query}")
            }
            Statement::AlterType(AlterType { name, operation }) => {
                write!(f, "ALTER TYPE {name} {operation}")
            }
            Statement::AlterOperator(alter_operator) => write!(f, "{alter_operator}"),
            Statement::AlterOperatorFamily(alter_operator_family) => {
                write!(f, "{alter_operator_family}")
            }
            Statement::AlterRole { name, operation } => {
                write!(f, "ALTER ROLE {name} {operation}")
            }
            Statement::AlterPolicy {
                name,
                table_name,
                operation,
            } => {
                write!(f, "ALTER POLICY {name} ON {table_name}{operation}")
            }
            Statement::AlterConnector {
                name,
                properties,
                url,
                owner,
            } => {
                write!(f, "ALTER CONNECTOR {name}")?;
                if let Some(properties) = properties {
                    write!(
                        f,
                        " SET DCPROPERTIES({})",
                        display_comma_separated(properties)
                    )?;
                }
                if let Some(url) = url {
                    write!(f, " SET URL '{url}'")?;
                }
                if let Some(owner) = owner {
                    write!(f, " SET OWNER {owner}")?;
                }
                Ok(())
            }
            Statement::AlterSession {
                set,
                session_params,
            } => {
                write!(
                    f,
                    "ALTER SESSION {set}",
                    set = if *set { "SET" } else { "UNSET" }
                )?;
                if !session_params.options.is_empty() {
                    if *set {
                        write!(f, " {session_params}")?;
                    } else {
                        let options = session_params
                            .options
                            .iter()
                            .map(|p| p.option_name.clone())
                            .collect::<Vec<_>>();
                        write!(f, " {}", display_separated(&options, ", "))?;
                    }
                }
                Ok(())
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
            } => {
                write!(
                    f,
                    "DROP {}{}{} {}{}{}{}",
                    if *temporary { "TEMPORARY " } else { "" },
                    object_type,
                    if *if_exists { " IF EXISTS" } else { "" },
                    display_comma_separated(names),
                    if *cascade { " CASCADE" } else { "" },
                    if *restrict { " RESTRICT" } else { "" },
                    if *purge { " PURGE" } else { "" },
                )?;
                if let Some(table_name) = table.as_ref() {
                    write!(f, " ON {table_name}")?;
                };
                Ok(())
            }
            Statement::DropFunction(drop_function) => write!(f, "{drop_function}"),
            Statement::DropDomain(DropDomain {
                if_exists,
                name,
                drop_behavior,
            }) => {
                write!(
                    f,
                    "DROP DOMAIN{} {name}",
                    if *if_exists { " IF EXISTS" } else { "" },
                )?;
                if let Some(op) = drop_behavior {
                    write!(f, " {op}")?;
                }
                Ok(())
            }
            Statement::DropProcedure {
                if_exists,
                proc_desc,
                drop_behavior,
            } => {
                write!(
                    f,
                    "DROP PROCEDURE{} {}",
                    if *if_exists { " IF EXISTS" } else { "" },
                    display_comma_separated(proc_desc),
                )?;
                if let Some(op) = drop_behavior {
                    write!(f, " {op}")?;
                }
                Ok(())
            }
            Statement::DropSecret {
                if_exists,
                temporary,
                name,
                storage_specifier,
            } => {
                write!(f, "DROP ")?;
                if let Some(t) = temporary {
                    write!(f, "{}", if *t { "TEMPORARY " } else { "PERSISTENT " })?;
                }
                write!(
                    f,
                    "SECRET {if_exists}{name}",
                    if_exists = if *if_exists { "IF EXISTS " } else { "" },
                )?;
                if let Some(s) = storage_specifier {
                    write!(f, " FROM {s}")?;
                }
                Ok(())
            }
            Statement::DropPolicy {
                if_exists,
                name,
                table_name,
                drop_behavior,
            } => {
                write!(f, "DROP POLICY")?;
                if *if_exists {
                    write!(f, " IF EXISTS")?;
                }
                write!(f, " {name} ON {table_name}")?;
                if let Some(drop_behavior) = drop_behavior {
                    write!(f, " {drop_behavior}")?;
                }
                Ok(())
            }
            Statement::DropConnector { if_exists, name } => {
                write!(
                    f,
                    "DROP CONNECTOR {if_exists}{name}",
                    if_exists = if *if_exists { "IF EXISTS " } else { "" }
                )?;
                Ok(())
            }
            Statement::Discard { object_type } => {
                write!(f, "DISCARD {object_type}")?;
                Ok(())
            }
            Self::Set(set) => write!(f, "{set}"),
            Statement::ShowVariable { variable } => {
                write!(f, "SHOW")?;
                if !variable.is_empty() {
                    write!(f, " {}", display_separated(variable, " "))?;
                }
                Ok(())
            }
            Statement::ShowStatus {
                filter,
                global,
                session,
            } => {
                write!(f, "SHOW")?;
                if *global {
                    write!(f, " GLOBAL")?;
                }
                if *session {
                    write!(f, " SESSION")?;
                }
                write!(f, " STATUS")?;
                if filter.is_some() {
                    write!(f, " {}", filter.as_ref().unwrap())?;
                }
                Ok(())
            }
            Statement::ShowVariables {
                filter,
                global,
                session,
            } => {
                write!(f, "SHOW")?;
                if *global {
                    write!(f, " GLOBAL")?;
                }
                if *session {
                    write!(f, " SESSION")?;
                }
                write!(f, " VARIABLES")?;
                if filter.is_some() {
                    write!(f, " {}", filter.as_ref().unwrap())?;
                }
                Ok(())
            }
            Statement::ShowCreate { obj_type, obj_name } => {
                write!(f, "SHOW CREATE {obj_type} {obj_name}",)?;
                Ok(())
            }
            Statement::ShowColumns {
                extended,
                full,
                show_options,
            } => {
                write!(
                    f,
                    "SHOW {extended}{full}COLUMNS{show_options}",
                    extended = if *extended { "EXTENDED " } else { "" },
                    full = if *full { "FULL " } else { "" },
                )?;
                Ok(())
            }
            Statement::ShowDatabases {
                terse,
                history,
                show_options,
            } => {
                write!(
                    f,
                    "SHOW {terse}DATABASES{history}{show_options}",
                    terse = if *terse { "TERSE " } else { "" },
                    history = if *history { " HISTORY" } else { "" },
                )?;
                Ok(())
            }
            Statement::ShowSchemas {
                terse,
                history,
                show_options,
            } => {
                write!(
                    f,
                    "SHOW {terse}SCHEMAS{history}{show_options}",
                    terse = if *terse { "TERSE " } else { "" },
                    history = if *history { " HISTORY" } else { "" },
                )?;
                Ok(())
            }
            Statement::ShowObjects(ShowObjects {
                terse,
                show_options,
            }) => {
                write!(
                    f,
                    "SHOW {terse}OBJECTS{show_options}",
                    terse = if *terse { "TERSE " } else { "" },
                )?;
                Ok(())
            }
            Statement::ShowTables {
                terse,
                history,
                extended,
                full,
                external,
                show_options,
            } => {
                write!(
                    f,
                    "SHOW {terse}{extended}{full}{external}TABLES{history}{show_options}",
                    terse = if *terse { "TERSE " } else { "" },
                    extended = if *extended { "EXTENDED " } else { "" },
                    full = if *full { "FULL " } else { "" },
                    external = if *external { "EXTERNAL " } else { "" },
                    history = if *history { " HISTORY" } else { "" },
                )?;
                Ok(())
            }
            Statement::ShowViews {
                terse,
                materialized,
                show_options,
            } => {
                write!(
                    f,
                    "SHOW {terse}{materialized}VIEWS{show_options}",
                    terse = if *terse { "TERSE " } else { "" },
                    materialized = if *materialized { "MATERIALIZED " } else { "" }
                )?;
                Ok(())
            }
            Statement::ShowFunctions { filter } => {
                write!(f, "SHOW FUNCTIONS")?;
                if let Some(filter) = filter {
                    write!(f, " {filter}")?;
                }
                Ok(())
            }
            Statement::Use(use_expr) => use_expr.fmt(f),
            Statement::ShowCollation { filter } => {
                write!(f, "SHOW COLLATION")?;
                if let Some(filter) = filter {
                    write!(f, " {filter}")?;
                }
                Ok(())
            }
            Statement::ShowCharset(show_stm) => show_stm.fmt(f),
            Statement::StartTransaction {
                modes,
                begin: syntax_begin,
                transaction,
                modifier,
                statements,
                exception,
                has_end_keyword,
            } => {
                if *syntax_begin {
                    if let Some(modifier) = *modifier {
                        write!(f, "BEGIN {modifier}")?;
                    } else {
                        write!(f, "BEGIN")?;
                    }
                } else {
                    write!(f, "START")?;
                }
                if let Some(transaction) = transaction {
                    write!(f, " {transaction}")?;
                }
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                if !statements.is_empty() {
                    write!(f, " ")?;
                    format_statement_list(f, statements)?;
                }
                if let Some(exception_when) = exception {
                    write!(f, " EXCEPTION")?;
                    for when in exception_when {
                        write!(f, " {when}")?;
                    }
                }
                if *has_end_keyword {
                    write!(f, " END")?;
                }
                Ok(())
            }
            Statement::Commit {
                chain,
                end: end_syntax,
                modifier,
            } => {
                if *end_syntax {
                    write!(f, "END")?;
                    if let Some(modifier) = *modifier {
                        write!(f, " {modifier}")?;
                    }
                    if *chain {
                        write!(f, " AND CHAIN")?;
                    }
                } else {
                    write!(f, "COMMIT{}", if *chain { " AND CHAIN" } else { "" })?;
                }
                Ok(())
            }
            Statement::Rollback { chain, savepoint } => {
                write!(f, "ROLLBACK")?;

                if *chain {
                    write!(f, " AND CHAIN")?;
                }

                if let Some(savepoint) = savepoint {
                    write!(f, " TO SAVEPOINT {savepoint}")?;
                }

                Ok(())
            }
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                with,
                options,
                default_collate_spec,
                clone,
            } => {
                write!(
                    f,
                    "CREATE SCHEMA {if_not_exists}{name}",
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    name = schema_name
                )?;

                if let Some(collate) = default_collate_spec {
                    write!(f, " DEFAULT COLLATE {collate}")?;
                }

                if let Some(with) = with {
                    write!(f, " WITH ({})", display_comma_separated(with))?;
                }

                if let Some(options) = options {
                    write!(f, " OPTIONS({})", display_comma_separated(options))?;
                }

                if let Some(clone) = clone {
                    write!(f, " CLONE {clone}")?;
                }
                Ok(())
            }
            Statement::Assert { condition, message } => {
                write!(f, "ASSERT {condition}")?;
                if let Some(m) = message {
                    write!(f, " AS {m}")?;
                }
                Ok(())
            }
            Statement::Grant {
                privileges,
                objects,
                grantees,
                with_grant_option,
                as_grantor,
                granted_by,
                current_grants,
            } => {
                write!(f, "GRANT {privileges} ")?;
                if let Some(objects) = objects {
                    write!(f, "ON {objects} ")?;
                }
                write!(f, "TO {}", display_comma_separated(grantees))?;
                if *with_grant_option {
                    write!(f, " WITH GRANT OPTION")?;
                }
                if let Some(current_grants) = current_grants {
                    write!(f, " {current_grants}")?;
                }
                if let Some(grantor) = as_grantor {
                    write!(f, " AS {grantor}")?;
                }
                if let Some(grantor) = granted_by {
                    write!(f, " GRANTED BY {grantor}")?;
                }
                Ok(())
            }
            Statement::Deny(s) => write!(f, "{s}"),
            Statement::Revoke {
                privileges,
                objects,
                grantees,
                granted_by,
                cascade,
            } => {
                write!(f, "REVOKE {privileges} ")?;
                if let Some(objects) = objects {
                    write!(f, "ON {objects} ")?;
                }
                write!(f, "FROM {}", display_comma_separated(grantees))?;
                if let Some(grantor) = granted_by {
                    write!(f, " GRANTED BY {grantor}")?;
                }
                if let Some(cascade) = cascade {
                    write!(f, " {cascade}")?;
                }
                Ok(())
            }
            Statement::Deallocate { name, prepare } => write!(
                f,
                "DEALLOCATE {prepare}{name}",
                prepare = if *prepare { "PREPARE " } else { "" },
                name = name,
            ),
            Statement::Execute {
                name,
                parameters,
                has_parentheses,
                immediate,
                into,
                using,
                output,
                default,
            } => {
                let (open, close) = if *has_parentheses {
                    ("(", ")")
                } else {
                    (if parameters.is_empty() { "" } else { " " }, "")
                };
                write!(f, "EXECUTE")?;
                if *immediate {
                    write!(f, " IMMEDIATE")?;
                }
                if let Some(name) = name {
                    write!(f, " {name}")?;
                }
                write!(f, "{open}{}{close}", display_comma_separated(parameters),)?;
                if !into.is_empty() {
                    write!(f, " INTO {}", display_comma_separated(into))?;
                }
                if !using.is_empty() {
                    write!(f, " USING {}", display_comma_separated(using))?;
                };
                if *output {
                    write!(f, " OUTPUT")?;
                }
                if *default {
                    write!(f, " DEFAULT")?;
                }
                Ok(())
            }
            Statement::Prepare {
                name,
                data_types,
                statement,
            } => {
                write!(f, "PREPARE {name} ")?;
                if !data_types.is_empty() {
                    write!(f, "({}) ", display_comma_separated(data_types))?;
                }
                write!(f, "AS {statement}")
            }
            Statement::Comment {
                object_type,
                object_name,
                comment,
                if_exists,
            } => {
                write!(f, "COMMENT ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?
                };
                write!(f, "ON {object_type} {object_name} IS ")?;
                if let Some(c) = comment {
                    write!(f, "'{c}'")
                } else {
                    write!(f, "NULL")
                }
            }
            Statement::Savepoint { name } => {
                write!(f, "SAVEPOINT ")?;
                write!(f, "{name}")
            }
            Statement::ReleaseSavepoint { name } => {
                write!(f, "RELEASE SAVEPOINT {name}")
            }
            Statement::Merge(merge) => merge.fmt(f),
            Statement::Cache {
                table_name,
                table_flag,
                has_as,
                options,
                query,
            } => {
                if let Some(table_flag) = table_flag {
                    write!(f, "CACHE {table_flag} TABLE {table_name}")?;
                } else {
                    write!(f, "CACHE TABLE {table_name}")?;
                }

                if !options.is_empty() {
                    write!(f, " OPTIONS({})", display_comma_separated(options))?;
                }

                match (*has_as, query) {
                    (true, Some(query)) => write!(f, " AS {query}"),
                    (true, None) => f.write_str(" AS"),
                    (false, Some(query)) => write!(f, " {query}"),
                    (false, None) => Ok(()),
                }
            }
            Statement::UNCache {
                table_name,
                if_exists,
            } => {
                if *if_exists {
                    write!(f, "UNCACHE TABLE IF EXISTS {table_name}")
                } else {
                    write!(f, "UNCACHE TABLE {table_name}")
                }
            }
            Statement::CreateSequence {
                temporary,
                if_not_exists,
                name,
                data_type,
                sequence_options,
                owned_by,
            } => {
                let as_type: String = if let Some(dt) = data_type.as_ref() {
                    //Cannot use format!(" AS {}", dt), due to format! is not available in --target thumbv6m-none-eabi
                    // " AS ".to_owned() + &dt.to_string()
                    [" AS ", &dt.to_string()].concat()
                } else {
                    "".to_string()
                };
                write!(
                    f,
                    "CREATE {temporary}SEQUENCE {if_not_exists}{name}{as_type}",
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    temporary = if *temporary { "TEMPORARY " } else { "" },
                    name = name,
                    as_type = as_type
                )?;
                for sequence_option in sequence_options {
                    write!(f, "{sequence_option}")?;
                }
                if let Some(ob) = owned_by.as_ref() {
                    write!(f, " OWNED BY {ob}")?;
                }
                write!(f, "")
            }
            Statement::CreateStage {
                or_replace,
                temporary,
                if_not_exists,
                name,
                stage_params,
                directory_table_params,
                file_format,
                copy_options,
                comment,
                ..
            } => {
                write!(
                    f,
                    "CREATE {or_replace}{temp}STAGE {if_not_exists}{name}{stage_params}",
                    temp = if *temporary { "TEMPORARY " } else { "" },
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                )?;
                if !directory_table_params.options.is_empty() {
                    write!(f, " DIRECTORY=({directory_table_params})")?;
                }
                if !file_format.options.is_empty() {
                    write!(f, " FILE_FORMAT=({file_format})")?;
                }
                if !copy_options.options.is_empty() {
                    write!(f, " COPY_OPTIONS=({copy_options})")?;
                }
                if comment.is_some() {
                    write!(f, " COMMENT='{}'", comment.as_ref().unwrap())?;
                }
                Ok(())
            }
            Statement::CopyIntoSnowflake {
                kind,
                into,
                into_columns,
                from_obj,
                from_obj_alias,
                stage_params,
                from_transformations,
                from_query,
                files,
                pattern,
                file_format,
                copy_options,
                validation_mode,
                partition,
            } => {
                write!(f, "COPY INTO {into}")?;
                if let Some(into_columns) = into_columns {
                    write!(f, " ({})", display_comma_separated(into_columns))?;
                }
                if let Some(from_transformations) = from_transformations {
                    // Data load with transformation
                    if let Some(from_stage) = from_obj {
                        write!(
                            f,
                            " FROM (SELECT {} FROM {}{}",
                            display_separated(from_transformations, ", "),
                            from_stage,
                            stage_params
                        )?;
                    }
                    if let Some(from_obj_alias) = from_obj_alias {
                        write!(f, " AS {from_obj_alias}")?;
                    }
                    write!(f, ")")?;
                } else if let Some(from_obj) = from_obj {
                    // Standard data load
                    write!(f, " FROM {from_obj}{stage_params}")?;
                    if let Some(from_obj_alias) = from_obj_alias {
                        write!(f, " AS {from_obj_alias}")?;
                    }
                } else if let Some(from_query) = from_query {
                    // Data unload from query
                    write!(f, " FROM ({from_query})")?;
                }

                if let Some(files) = files {
                    write!(f, " FILES = ('{}')", display_separated(files, "', '"))?;
                }
                if let Some(pattern) = pattern {
                    write!(f, " PATTERN = '{pattern}'")?;
                }
                if let Some(partition) = partition {
                    write!(f, " PARTITION BY {partition}")?;
                }
                if !file_format.options.is_empty() {
                    write!(f, " FILE_FORMAT=({file_format})")?;
                }
                if !copy_options.options.is_empty() {
                    match kind {
                        CopyIntoSnowflakeKind::Table => {
                            write!(f, " COPY_OPTIONS=({copy_options})")?
                        }
                        CopyIntoSnowflakeKind::Location => write!(f, " {copy_options}")?,
                    }
                }
                if let Some(validation_mode) = validation_mode {
                    write!(f, " VALIDATION_MODE = {validation_mode}")?;
                }
                Ok(())
            }
            Statement::CreateType {
                name,
                representation,
            } => {
                write!(f, "CREATE TYPE {name}")?;
                if let Some(repr) = representation {
                    write!(f, " {repr}")?;
                }
                Ok(())
            }
            Statement::Pragma { name, value, is_eq } => {
                write!(f, "PRAGMA {name}")?;
                if value.is_some() {
                    let val = value.as_ref().unwrap();
                    if *is_eq {
                        write!(f, " = {val}")?;
                    } else {
                        write!(f, "({val})")?;
                    }
                }
                Ok(())
            }
            Statement::LockTables { tables } => {
                write!(f, "LOCK TABLES {}", display_comma_separated(tables))
            }
            Statement::UnlockTables => {
                write!(f, "UNLOCK TABLES")
            }
            Statement::Unload {
                query,
                query_text,
                to,
                auth,
                with,
                options,
            } => {
                write!(f, "UNLOAD(")?;
                if let Some(query) = query {
                    write!(f, "{query}")?;
                }
                if let Some(query_text) = query_text {
                    write!(f, "'{query_text}'")?;
                }
                write!(f, ") TO {to}")?;
                if let Some(auth) = auth {
                    write!(f, " IAM_ROLE {auth}")?;
                }
                if !with.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with))?;
                }
                if !options.is_empty() {
                    write!(f, " {}", display_separated(options, " "))?;
                }
                Ok(())
            }
            Statement::OptimizeTable {
                name,
                on_cluster,
                partition,
                include_final,
                deduplicate,
            } => {
                write!(f, "OPTIMIZE TABLE {name}")?;
                if let Some(on_cluster) = on_cluster {
                    write!(f, " ON CLUSTER {on_cluster}")?;
                }
                if let Some(partition) = partition {
                    write!(f, " {partition}")?;
                }
                if *include_final {
                    write!(f, " FINAL")?;
                }
                if let Some(deduplicate) = deduplicate {
                    write!(f, " {deduplicate}")?;
                }
                Ok(())
            }
            Statement::LISTEN { channel } => {
                write!(f, "LISTEN {channel}")?;
                Ok(())
            }
            Statement::UNLISTEN { channel } => {
                write!(f, "UNLISTEN {channel}")?;
                Ok(())
            }
            Statement::NOTIFY { channel, payload } => {
                write!(f, "NOTIFY {channel}")?;
                if let Some(payload) = payload {
                    write!(f, ", '{payload}'")?;
                }
                Ok(())
            }
            Statement::RenameTable(rename_tables) => {
                write!(f, "RENAME TABLE {}", display_comma_separated(rename_tables))
            }
            Statement::RaisError {
                message,
                severity,
                state,
                arguments,
                options,
            } => {
                write!(f, "RAISERROR({message}, {severity}, {state}")?;
                if !arguments.is_empty() {
                    write!(f, ", {}", display_comma_separated(arguments))?;
                }
                write!(f, ")")?;
                if !options.is_empty() {
                    write!(f, " WITH {}", display_comma_separated(options))?;
                }
                Ok(())
            }
            Statement::Print(s) => write!(f, "{s}"),
            Statement::Return(r) => write!(f, "{r}"),
            Statement::List(command) => write!(f, "LIST {command}"),
            Statement::Remove(command) => write!(f, "REMOVE {command}"),
            Statement::ExportData(e) => write!(f, "{e}"),
            Statement::CreateUser(s) => write!(f, "{s}"),
            Statement::AlterSchema(s) => write!(f, "{s}"),
            Statement::Vacuum(s) => write!(f, "{s}"),
            Statement::AlterUser(s) => write!(f, "{s}"),
            Statement::Reset(s) => write!(f, "{s}"),
        }
    }
}

/// Can use to describe options in create sequence or table column type identity
/// ```sql
/// [ INCREMENT [ BY ] increment ]
///     [ MINVALUE minvalue | NO MINVALUE ] [ MAXVALUE maxvalue | NO MAXVALUE ]
///     [ START [ WITH ] start ] [ CACHE cache ] [ [ NO ] CYCLE ]
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SequenceOptions {
    IncrementBy(Expr, bool),
    MinValue(Option<Expr>),
    MaxValue(Option<Expr>),
    StartWith(Expr, bool),
    Cache(Expr),
    Cycle(bool),
}

impl fmt::Display for SequenceOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SequenceOptions::IncrementBy(increment, by) => {
                write!(
                    f,
                    " INCREMENT{by} {increment}",
                    by = if *by { " BY" } else { "" },
                    increment = increment
                )
            }
            SequenceOptions::MinValue(Some(expr)) => {
                write!(f, " MINVALUE {expr}")
            }
            SequenceOptions::MinValue(None) => {
                write!(f, " NO MINVALUE")
            }
            SequenceOptions::MaxValue(Some(expr)) => {
                write!(f, " MAXVALUE {expr}")
            }
            SequenceOptions::MaxValue(None) => {
                write!(f, " NO MAXVALUE")
            }
            SequenceOptions::StartWith(start, with) => {
                write!(
                    f,
                    " START{with} {start}",
                    with = if *with { " WITH" } else { "" },
                    start = start
                )
            }
            SequenceOptions::Cache(cache) => {
                write!(f, " CACHE {}", *cache)
            }
            SequenceOptions::Cycle(no) => {
                write!(f, " {}CYCLE", if *no { "NO " } else { "" })
            }
        }
    }
}

/// Assignment for a `SET` statement (name [=|TO] value)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SetAssignment {
    pub scope: Option<ContextModifier>,
    pub name: ObjectName,
    pub value: Expr,
}

impl fmt::Display for SetAssignment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}{} = {}",
            self.scope.map(|s| format!("{s}")).unwrap_or_default(),
            self.name,
            self.value
        )
    }
}

/// Target of a `TRUNCATE TABLE` command
///
/// Note this is its own struct because `visit_relation` requires an `ObjectName` (not a `Vec<ObjectName>`)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TruncateTableTarget {
    /// name of the table being truncated
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub name: ObjectName,
    /// Postgres-specific option
    /// [ TRUNCATE TABLE ONLY ]
    /// <https://www.postgresql.org/docs/current/sql-truncate.html>
    pub only: bool,
}

impl fmt::Display for TruncateTableTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.only {
            write!(f, "ONLY ")?;
        };
        write!(f, "{}", self.name)
    }
}

/// PostgreSQL identity option for TRUNCATE table
/// [ RESTART IDENTITY | CONTINUE IDENTITY ]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TruncateIdentityOption {
    Restart,
    Continue,
}

/// Cascade/restrict option for Postgres TRUNCATE table, MySQL GRANT/REVOKE, etc.
/// [ CASCADE | RESTRICT ]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CascadeOption {
    Cascade,
    Restrict,
}

impl Display for CascadeOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CascadeOption::Cascade => write!(f, "CASCADE"),
            CascadeOption::Restrict => write!(f, "RESTRICT"),
        }
    }
}

/// Transaction started with [ TRANSACTION | WORK ]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum BeginTransactionKind {
    Transaction,
    Work,
}

impl Display for BeginTransactionKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BeginTransactionKind::Transaction => write!(f, "TRANSACTION"),
            BeginTransactionKind::Work => write!(f, "WORK"),
        }
    }
}

/// Can use to describe options in  create sequence or table column type identity
/// [ MINVALUE minvalue | NO MINVALUE ] [ MAXVALUE maxvalue | NO MAXVALUE ]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MinMaxValue {
    // clause is not specified
    Empty,
    // NO MINVALUE/NO MAXVALUE
    None,
    // MINVALUE <expr> / MAXVALUE <expr>
    Some(Expr),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[non_exhaustive]
pub enum OnInsert {
    /// ON DUPLICATE KEY UPDATE (MySQL when the key already exists, then execute an update instead)
    DuplicateKeyUpdate(Vec<Assignment>),
    /// ON CONFLICT is a PostgreSQL and Sqlite extension
    OnConflict(OnConflict),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct InsertAliases {
    pub row_alias: ObjectName,
    pub col_aliases: Option<Vec<Ident>>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OnConflict {
    pub conflict_target: Option<ConflictTarget>,
    pub action: OnConflictAction,
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ConflictTarget {
    Columns(Vec<Ident>),
    OnConstraint(ObjectName),
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate(DoUpdate),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DoUpdate {
    /// Column assignments
    pub assignments: Vec<Assignment>,
    /// WHERE
    pub selection: Option<Expr>,
}

impl fmt::Display for OnInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::DuplicateKeyUpdate(expr) => write!(
                f,
                " ON DUPLICATE KEY UPDATE {}",
                display_comma_separated(expr)
            ),
            Self::OnConflict(o) => write!(f, "{o}"),
        }
    }
}
impl fmt::Display for OnConflict {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, " ON CONFLICT")?;
        if let Some(target) = &self.conflict_target {
            write!(f, "{target}")?;
        }
        write!(f, " {}", self.action)
    }
}
impl fmt::Display for ConflictTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConflictTarget::Columns(cols) => write!(f, "({})", display_comma_separated(cols)),
            ConflictTarget::OnConstraint(name) => write!(f, " ON CONSTRAINT {name}"),
        }
    }
}
impl fmt::Display for OnConflictAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::DoNothing => write!(f, "DO NOTHING"),
            Self::DoUpdate(do_update) => {
                write!(f, "DO UPDATE")?;
                if !do_update.assignments.is_empty() {
                    write!(
                        f,
                        " SET {}",
                        display_comma_separated(&do_update.assignments)
                    )?;
                }
                if let Some(selection) = &do_update.selection {
                    write!(f, " WHERE {selection}")?;
                }
                Ok(())
            }
        }
    }
}

/// Privileges granted in a GRANT statement or revoked in a REVOKE statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Privileges {
    /// All privileges applicable to the object type
    All {
        /// Optional keyword from the spec, ignored in practice
        with_privileges_keyword: bool,
    },
    /// Specific privileges (e.g. `SELECT`, `INSERT`)
    Actions(Vec<Action>),
}

impl fmt::Display for Privileges {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Privileges::All {
                with_privileges_keyword,
            } => {
                write!(
                    f,
                    "ALL{}",
                    if *with_privileges_keyword {
                        " PRIVILEGES"
                    } else {
                        ""
                    }
                )
            }
            Privileges::Actions(actions) => {
                write!(f, "{}", display_comma_separated(actions))
            }
        }
    }
}

/// Specific direction for FETCH statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FetchDirection {
    Count { limit: Value },
    Next,
    Prior,
    First,
    Last,
    Absolute { limit: Value },
    Relative { limit: Value },
    All,
    // FORWARD
    // FORWARD count
    Forward { limit: Option<Value> },
    ForwardAll,
    // BACKWARD
    // BACKWARD count
    Backward { limit: Option<Value> },
    BackwardAll,
}

impl fmt::Display for FetchDirection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FetchDirection::Count { limit } => f.write_str(&limit.to_string())?,
            FetchDirection::Next => f.write_str("NEXT")?,
            FetchDirection::Prior => f.write_str("PRIOR")?,
            FetchDirection::First => f.write_str("FIRST")?,
            FetchDirection::Last => f.write_str("LAST")?,
            FetchDirection::Absolute { limit } => {
                f.write_str("ABSOLUTE ")?;
                f.write_str(&limit.to_string())?;
            }
            FetchDirection::Relative { limit } => {
                f.write_str("RELATIVE ")?;
                f.write_str(&limit.to_string())?;
            }
            FetchDirection::All => f.write_str("ALL")?,
            FetchDirection::Forward { limit } => {
                f.write_str("FORWARD")?;

                if let Some(l) = limit {
                    f.write_str(" ")?;
                    f.write_str(&l.to_string())?;
                }
            }
            FetchDirection::ForwardAll => f.write_str("FORWARD ALL")?,
            FetchDirection::Backward { limit } => {
                f.write_str("BACKWARD")?;

                if let Some(l) = limit {
                    f.write_str(" ")?;
                    f.write_str(&l.to_string())?;
                }
            }
            FetchDirection::BackwardAll => f.write_str("BACKWARD ALL")?,
        };

        Ok(())
    }
}

/// The "position" for a FETCH statement.
///
/// [MsSql](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/fetch-transact-sql)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FetchPosition {
    From,
    In,
}

impl fmt::Display for FetchPosition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FetchPosition::From => f.write_str("FROM")?,
            FetchPosition::In => f.write_str("IN")?,
        };

        Ok(())
    }
}

/// A privilege on a database object (table, sequence, etc.).
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Action {
    AddSearchOptimization,
    Apply {
        apply_type: ActionApplyType,
    },
    ApplyBudget,
    AttachListing,
    AttachPolicy,
    Audit,
    BindServiceEndpoint,
    Connect,
    Create {
        obj_type: Option<ActionCreateObjectType>,
    },
    DatabaseRole {
        role: ObjectName,
    },
    Delete,
    Drop,
    EvolveSchema,
    Exec {
        obj_type: Option<ActionExecuteObjectType>,
    },
    Execute {
        obj_type: Option<ActionExecuteObjectType>,
    },
    Failover,
    ImportedPrivileges,
    ImportShare,
    Insert {
        columns: Option<Vec<Ident>>,
    },
    Manage {
        manage_type: ActionManageType,
    },
    ManageReleases,
    ManageVersions,
    Modify {
        modify_type: Option<ActionModifyType>,
    },
    Monitor {
        monitor_type: Option<ActionMonitorType>,
    },
    Operate,
    OverrideShareRestrictions,
    Ownership,
    PurchaseDataExchangeListing,
    Read,
    ReadSession,
    References {
        columns: Option<Vec<Ident>>,
    },
    Replicate,
    ResolveAll,
    Role {
        role: ObjectName,
    },
    Select {
        columns: Option<Vec<Ident>>,
    },
    Temporary,
    Trigger,
    Truncate,
    Update {
        columns: Option<Vec<Ident>>,
    },
    Usage,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Action::AddSearchOptimization => f.write_str("ADD SEARCH OPTIMIZATION")?,
            Action::Apply { apply_type } => write!(f, "APPLY {apply_type}")?,
            Action::ApplyBudget => f.write_str("APPLYBUDGET")?,
            Action::AttachListing => f.write_str("ATTACH LISTING")?,
            Action::AttachPolicy => f.write_str("ATTACH POLICY")?,
            Action::Audit => f.write_str("AUDIT")?,
            Action::BindServiceEndpoint => f.write_str("BIND SERVICE ENDPOINT")?,
            Action::Connect => f.write_str("CONNECT")?,
            Action::Create { obj_type } => {
                f.write_str("CREATE")?;
                if let Some(obj_type) = obj_type {
                    write!(f, " {obj_type}")?
                }
            }
            Action::DatabaseRole { role } => write!(f, "DATABASE ROLE {role}")?,
            Action::Delete => f.write_str("DELETE")?,
            Action::Drop => f.write_str("DROP")?,
            Action::EvolveSchema => f.write_str("EVOLVE SCHEMA")?,
            Action::Exec { obj_type } => {
                f.write_str("EXEC")?;
                if let Some(obj_type) = obj_type {
                    write!(f, " {obj_type}")?
                }
            }
            Action::Execute { obj_type } => {
                f.write_str("EXECUTE")?;
                if let Some(obj_type) = obj_type {
                    write!(f, " {obj_type}")?
                }
            }
            Action::Failover => f.write_str("FAILOVER")?,
            Action::ImportedPrivileges => f.write_str("IMPORTED PRIVILEGES")?,
            Action::ImportShare => f.write_str("IMPORT SHARE")?,
            Action::Insert { .. } => f.write_str("INSERT")?,
            Action::Manage { manage_type } => write!(f, "MANAGE {manage_type}")?,
            Action::ManageReleases => f.write_str("MANAGE RELEASES")?,
            Action::ManageVersions => f.write_str("MANAGE VERSIONS")?,
            Action::Modify { modify_type } => {
                write!(f, "MODIFY")?;
                if let Some(modify_type) = modify_type {
                    write!(f, " {modify_type}")?;
                }
            }
            Action::Monitor { monitor_type } => {
                write!(f, "MONITOR")?;
                if let Some(monitor_type) = monitor_type {
                    write!(f, " {monitor_type}")?
                }
            }
            Action::Operate => f.write_str("OPERATE")?,
            Action::OverrideShareRestrictions => f.write_str("OVERRIDE SHARE RESTRICTIONS")?,
            Action::Ownership => f.write_str("OWNERSHIP")?,
            Action::PurchaseDataExchangeListing => f.write_str("PURCHASE DATA EXCHANGE LISTING")?,
            Action::Read => f.write_str("READ")?,
            Action::ReadSession => f.write_str("READ SESSION")?,
            Action::References { .. } => f.write_str("REFERENCES")?,
            Action::Replicate => f.write_str("REPLICATE")?,
            Action::ResolveAll => f.write_str("RESOLVE ALL")?,
            Action::Role { role } => write!(f, "ROLE {role}")?,
            Action::Select { .. } => f.write_str("SELECT")?,
            Action::Temporary => f.write_str("TEMPORARY")?,
            Action::Trigger => f.write_str("TRIGGER")?,
            Action::Truncate => f.write_str("TRUNCATE")?,
            Action::Update { .. } => f.write_str("UPDATE")?,
            Action::Usage => f.write_str("USAGE")?,
        };
        match self {
            Action::Insert { columns }
            | Action::References { columns }
            | Action::Select { columns }
            | Action::Update { columns } => {
                if let Some(columns) = columns {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
            }
            _ => (),
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// See <https://docs.snowflake.com/en/sql-reference/sql/grant-privilege>
/// under `globalPrivileges` in the `CREATE` privilege.
pub enum ActionCreateObjectType {
    Account,
    Application,
    ApplicationPackage,
    ComputePool,
    DataExchangeListing,
    Database,
    ExternalVolume,
    FailoverGroup,
    Integration,
    NetworkPolicy,
    OrganiationListing,
    ReplicationGroup,
    Role,
    Schema,
    Share,
    User,
    Warehouse,
}

impl fmt::Display for ActionCreateObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ActionCreateObjectType::Account => write!(f, "ACCOUNT"),
            ActionCreateObjectType::Application => write!(f, "APPLICATION"),
            ActionCreateObjectType::ApplicationPackage => write!(f, "APPLICATION PACKAGE"),
            ActionCreateObjectType::ComputePool => write!(f, "COMPUTE POOL"),
            ActionCreateObjectType::DataExchangeListing => write!(f, "DATA EXCHANGE LISTING"),
            ActionCreateObjectType::Database => write!(f, "DATABASE"),
            ActionCreateObjectType::ExternalVolume => write!(f, "EXTERNAL VOLUME"),
            ActionCreateObjectType::FailoverGroup => write!(f, "FAILOVER GROUP"),
            ActionCreateObjectType::Integration => write!(f, "INTEGRATION"),
            ActionCreateObjectType::NetworkPolicy => write!(f, "NETWORK POLICY"),
            ActionCreateObjectType::OrganiationListing => write!(f, "ORGANIZATION LISTING"),
            ActionCreateObjectType::ReplicationGroup => write!(f, "REPLICATION GROUP"),
            ActionCreateObjectType::Role => write!(f, "ROLE"),
            ActionCreateObjectType::Schema => write!(f, "SCHEMA"),
            ActionCreateObjectType::Share => write!(f, "SHARE"),
            ActionCreateObjectType::User => write!(f, "USER"),
            ActionCreateObjectType::Warehouse => write!(f, "WAREHOUSE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// See <https://docs.snowflake.com/en/sql-reference/sql/grant-privilege>
/// under `globalPrivileges` in the `APPLY` privilege.
pub enum ActionApplyType {
    AggregationPolicy,
    AuthenticationPolicy,
    JoinPolicy,
    MaskingPolicy,
    PackagesPolicy,
    PasswordPolicy,
    ProjectionPolicy,
    RowAccessPolicy,
    SessionPolicy,
    Tag,
}

impl fmt::Display for ActionApplyType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ActionApplyType::AggregationPolicy => write!(f, "AGGREGATION POLICY"),
            ActionApplyType::AuthenticationPolicy => write!(f, "AUTHENTICATION POLICY"),
            ActionApplyType::JoinPolicy => write!(f, "JOIN POLICY"),
            ActionApplyType::MaskingPolicy => write!(f, "MASKING POLICY"),
            ActionApplyType::PackagesPolicy => write!(f, "PACKAGES POLICY"),
            ActionApplyType::PasswordPolicy => write!(f, "PASSWORD POLICY"),
            ActionApplyType::ProjectionPolicy => write!(f, "PROJECTION POLICY"),
            ActionApplyType::RowAccessPolicy => write!(f, "ROW ACCESS POLICY"),
            ActionApplyType::SessionPolicy => write!(f, "SESSION POLICY"),
            ActionApplyType::Tag => write!(f, "TAG"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// See <https://docs.snowflake.com/en/sql-reference/sql/grant-privilege>
/// under `globalPrivileges` in the `EXECUTE` privilege.
pub enum ActionExecuteObjectType {
    Alert,
    DataMetricFunction,
    ManagedAlert,
    ManagedTask,
    Task,
}

impl fmt::Display for ActionExecuteObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ActionExecuteObjectType::Alert => write!(f, "ALERT"),
            ActionExecuteObjectType::DataMetricFunction => write!(f, "DATA METRIC FUNCTION"),
            ActionExecuteObjectType::ManagedAlert => write!(f, "MANAGED ALERT"),
            ActionExecuteObjectType::ManagedTask => write!(f, "MANAGED TASK"),
            ActionExecuteObjectType::Task => write!(f, "TASK"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// See <https://docs.snowflake.com/en/sql-reference/sql/grant-privilege>
/// under `globalPrivileges` in the `MANAGE` privilege.
pub enum ActionManageType {
    AccountSupportCases,
    EventSharing,
    Grants,
    ListingAutoFulfillment,
    OrganizationSupportCases,
    UserSupportCases,
    Warehouses,
}

impl fmt::Display for ActionManageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ActionManageType::AccountSupportCases => write!(f, "ACCOUNT SUPPORT CASES"),
            ActionManageType::EventSharing => write!(f, "EVENT SHARING"),
            ActionManageType::Grants => write!(f, "GRANTS"),
            ActionManageType::ListingAutoFulfillment => write!(f, "LISTING AUTO FULFILLMENT"),
            ActionManageType::OrganizationSupportCases => write!(f, "ORGANIZATION SUPPORT CASES"),
            ActionManageType::UserSupportCases => write!(f, "USER SUPPORT CASES"),
            ActionManageType::Warehouses => write!(f, "WAREHOUSES"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// See <https://docs.snowflake.com/en/sql-reference/sql/grant-privilege>
/// under `globalPrivileges` in the `MODIFY` privilege.
pub enum ActionModifyType {
    LogLevel,
    TraceLevel,
    SessionLogLevel,
    SessionTraceLevel,
}

impl fmt::Display for ActionModifyType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ActionModifyType::LogLevel => write!(f, "LOG LEVEL"),
            ActionModifyType::TraceLevel => write!(f, "TRACE LEVEL"),
            ActionModifyType::SessionLogLevel => write!(f, "SESSION LOG LEVEL"),
            ActionModifyType::SessionTraceLevel => write!(f, "SESSION TRACE LEVEL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// See <https://docs.snowflake.com/en/sql-reference/sql/grant-privilege>
/// under `globalPrivileges` in the `MONITOR` privilege.
pub enum ActionMonitorType {
    Execution,
    Security,
    Usage,
}

impl fmt::Display for ActionMonitorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ActionMonitorType::Execution => write!(f, "EXECUTION"),
            ActionMonitorType::Security => write!(f, "SECURITY"),
            ActionMonitorType::Usage => write!(f, "USAGE"),
        }
    }
}

/// The principal that receives the privileges
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Grantee {
    pub grantee_type: GranteesType,
    pub name: Option<GranteeName>,
}

impl fmt::Display for Grantee {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.grantee_type {
            GranteesType::Role => {
                write!(f, "ROLE ")?;
            }
            GranteesType::Share => {
                write!(f, "SHARE ")?;
            }
            GranteesType::User => {
                write!(f, "USER ")?;
            }
            GranteesType::Group => {
                write!(f, "GROUP ")?;
            }
            GranteesType::Public => {
                write!(f, "PUBLIC ")?;
            }
            GranteesType::DatabaseRole => {
                write!(f, "DATABASE ROLE ")?;
            }
            GranteesType::Application => {
                write!(f, "APPLICATION ")?;
            }
            GranteesType::ApplicationRole => {
                write!(f, "APPLICATION ROLE ")?;
            }
            GranteesType::None => (),
        }
        if let Some(ref name) = self.name {
            name.fmt(f)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GranteesType {
    Role,
    Share,
    User,
    Group,
    Public,
    DatabaseRole,
    Application,
    ApplicationRole,
    None,
}

/// Users/roles designated in a GRANT/REVOKE
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GranteeName {
    /// A bare identifier
    ObjectName(ObjectName),
    /// A MySQL user/host pair such as 'root'@'%'
    UserHost { user: Ident, host: Ident },
}

impl fmt::Display for GranteeName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GranteeName::ObjectName(name) => name.fmt(f),
            GranteeName::UserHost { user, host } => {
                write!(f, "{user}@{host}")
            }
        }
    }
}

/// Objects on which privileges are granted in a GRANT statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GrantObjects {
    /// Grant privileges on `ALL SEQUENCES IN SCHEMA <schema_name> [, ...]`
    AllSequencesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL TABLES IN SCHEMA <schema_name> [, ...]`
    AllTablesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL VIEWS IN SCHEMA <schema_name> [, ...]`
    AllViewsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL MATERIALIZED VIEWS IN SCHEMA <schema_name> [, ...]`
    AllMaterializedViewsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL EXTERNAL TABLES IN SCHEMA <schema_name> [, ...]`
    AllExternalTablesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL FUNCTIONS IN SCHEMA <schema_name> [, ...]`
    AllFunctionsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `FUTURE SCHEMAS IN DATABASE <database_name> [, ...]`
    FutureSchemasInDatabase { databases: Vec<ObjectName> },
    /// Grant privileges on `FUTURE TABLES IN SCHEMA <schema_name> [, ...]`
    FutureTablesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `FUTURE VIEWS IN SCHEMA <schema_name> [, ...]`
    FutureViewsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `FUTURE EXTERNAL TABLES IN SCHEMA <schema_name> [, ...]`
    FutureExternalTablesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `FUTURE MATERIALIZED VIEWS IN SCHEMA <schema_name> [, ...]`
    FutureMaterializedViewsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `FUTURE SEQUENCES IN SCHEMA <schema_name> [, ...]`
    FutureSequencesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on specific databases
    Databases(Vec<ObjectName>),
    /// Grant privileges on specific schemas
    Schemas(Vec<ObjectName>),
    /// Grant privileges on specific sequences
    Sequences(Vec<ObjectName>),
    /// Grant privileges on specific tables
    Tables(Vec<ObjectName>),
    /// Grant privileges on specific views
    Views(Vec<ObjectName>),
    /// Grant privileges on specific warehouses
    Warehouses(Vec<ObjectName>),
    /// Grant privileges on specific integrations
    Integrations(Vec<ObjectName>),
    /// Grant privileges on resource monitors
    ResourceMonitors(Vec<ObjectName>),
    /// Grant privileges on users
    Users(Vec<ObjectName>),
    /// Grant privileges on compute pools
    ComputePools(Vec<ObjectName>),
    /// Grant privileges on connections
    Connections(Vec<ObjectName>),
    /// Grant privileges on failover groups
    FailoverGroup(Vec<ObjectName>),
    /// Grant privileges on replication group
    ReplicationGroup(Vec<ObjectName>),
    /// Grant privileges on external volumes
    ExternalVolumes(Vec<ObjectName>),
    /// Grant privileges on a procedure. In dialects that
    /// support overloading, the argument types must be specified.
    ///
    /// For example:
    /// `GRANT USAGE ON PROCEDURE foo(varchar) TO ROLE role1`
    Procedure {
        name: ObjectName,
        arg_types: Vec<DataType>,
    },

    /// Grant privileges on a function. In dialects that
    /// support overloading, the argument types must be specified.
    ///
    /// For example:
    /// `GRANT USAGE ON FUNCTION foo(varchar) TO ROLE role1`
    Function {
        name: ObjectName,
        arg_types: Vec<DataType>,
    },
}

impl fmt::Display for GrantObjects {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GrantObjects::Sequences(sequences) => {
                write!(f, "SEQUENCE {}", display_comma_separated(sequences))
            }
            GrantObjects::Databases(databases) => {
                write!(f, "DATABASE {}", display_comma_separated(databases))
            }
            GrantObjects::Schemas(schemas) => {
                write!(f, "SCHEMA {}", display_comma_separated(schemas))
            }
            GrantObjects::Tables(tables) => {
                write!(f, "{}", display_comma_separated(tables))
            }
            GrantObjects::Views(views) => {
                write!(f, "VIEW {}", display_comma_separated(views))
            }
            GrantObjects::Warehouses(warehouses) => {
                write!(f, "WAREHOUSE {}", display_comma_separated(warehouses))
            }
            GrantObjects::Integrations(integrations) => {
                write!(f, "INTEGRATION {}", display_comma_separated(integrations))
            }
            GrantObjects::AllSequencesInSchema { schemas } => {
                write!(
                    f,
                    "ALL SEQUENCES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllTablesInSchema { schemas } => {
                write!(
                    f,
                    "ALL TABLES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllExternalTablesInSchema { schemas } => {
                write!(
                    f,
                    "ALL EXTERNAL TABLES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllViewsInSchema { schemas } => {
                write!(
                    f,
                    "ALL VIEWS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllMaterializedViewsInSchema { schemas } => {
                write!(
                    f,
                    "ALL MATERIALIZED VIEWS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllFunctionsInSchema { schemas } => {
                write!(
                    f,
                    "ALL FUNCTIONS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::FutureSchemasInDatabase { databases } => {
                write!(
                    f,
                    "FUTURE SCHEMAS IN DATABASE {}",
                    display_comma_separated(databases)
                )
            }
            GrantObjects::FutureTablesInSchema { schemas } => {
                write!(
                    f,
                    "FUTURE TABLES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::FutureExternalTablesInSchema { schemas } => {
                write!(
                    f,
                    "FUTURE EXTERNAL TABLES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::FutureViewsInSchema { schemas } => {
                write!(
                    f,
                    "FUTURE VIEWS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::FutureMaterializedViewsInSchema { schemas } => {
                write!(
                    f,
                    "FUTURE MATERIALIZED VIEWS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::FutureSequencesInSchema { schemas } => {
                write!(
                    f,
                    "FUTURE SEQUENCES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::ResourceMonitors(objects) => {
                write!(f, "RESOURCE MONITOR {}", display_comma_separated(objects))
            }
            GrantObjects::Users(objects) => {
                write!(f, "USER {}", display_comma_separated(objects))
            }
            GrantObjects::ComputePools(objects) => {
                write!(f, "COMPUTE POOL {}", display_comma_separated(objects))
            }
            GrantObjects::Connections(objects) => {
                write!(f, "CONNECTION {}", display_comma_separated(objects))
            }
            GrantObjects::FailoverGroup(objects) => {
                write!(f, "FAILOVER GROUP {}", display_comma_separated(objects))
            }
            GrantObjects::ReplicationGroup(objects) => {
                write!(f, "REPLICATION GROUP {}", display_comma_separated(objects))
            }
            GrantObjects::ExternalVolumes(objects) => {
                write!(f, "EXTERNAL VOLUME {}", display_comma_separated(objects))
            }
            GrantObjects::Procedure { name, arg_types } => {
                write!(f, "PROCEDURE {name}")?;
                if !arg_types.is_empty() {
                    write!(f, "({})", display_comma_separated(arg_types))?;
                }
                Ok(())
            }
            GrantObjects::Function { name, arg_types } => {
                write!(f, "FUNCTION {name}")?;
                if !arg_types.is_empty() {
                    write!(f, "({})", display_comma_separated(arg_types))?;
                }
                Ok(())
            }
        }
    }
}

/// A `DENY` statement
///
/// [MsSql](https://learn.microsoft.com/en-us/sql/t-sql/statements/deny-transact-sql)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DenyStatement {
    pub privileges: Privileges,
    pub objects: GrantObjects,
    pub grantees: Vec<Grantee>,
    pub granted_by: Option<Ident>,
    pub cascade: Option<CascadeOption>,
}

impl fmt::Display for DenyStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DENY {}", self.privileges)?;
        write!(f, " ON {}", self.objects)?;
        if !self.grantees.is_empty() {
            write!(f, " TO {}", display_comma_separated(&self.grantees))?;
        }
        if let Some(cascade) = &self.cascade {
            write!(f, " {cascade}")?;
        }
        if let Some(granted_by) = &self.granted_by {
            write!(f, " AS {granted_by}")?;
        }
        Ok(())
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Assignment {
    pub target: AssignmentTarget,
    pub value: Expr,
}

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", self.target, self.value)
    }
}

/// Left-hand side of an assignment in an UPDATE statement,
/// e.g. `foo` in `foo = 5` (ColumnName assignment) or
/// `(a, b)` in `(a, b) = (1, 2)` (Tuple assignment).
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AssignmentTarget {
    /// A single column
    ColumnName(ObjectName),
    /// A tuple of columns
    Tuple(Vec<ObjectName>),
}

impl fmt::Display for AssignmentTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AssignmentTarget::ColumnName(column) => write!(f, "{column}"),
            AssignmentTarget::Tuple(columns) => write!(f, "({})", display_comma_separated(columns)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FunctionArgExpr {
    Expr(Expr),
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    QualifiedWildcard(ObjectName),
    /// An unqualified `*`
    Wildcard,
}

impl From<Expr> for FunctionArgExpr {
    fn from(wildcard_expr: Expr) -> Self {
        match wildcard_expr {
            Expr::QualifiedWildcard(prefix, _) => Self::QualifiedWildcard(prefix),
            Expr::Wildcard(_) => Self::Wildcard,
            expr => Self::Expr(expr),
        }
    }
}

impl fmt::Display for FunctionArgExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionArgExpr::Expr(expr) => write!(f, "{expr}"),
            FunctionArgExpr::QualifiedWildcard(prefix) => write!(f, "{prefix}.*"),
            FunctionArgExpr::Wildcard => f.write_str("*"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// Operator used to separate function arguments
pub enum FunctionArgOperator {
    /// function(arg1 = value1)
    Equals,
    /// function(arg1 => value1)
    RightArrow,
    /// function(arg1 := value1)
    Assignment,
    /// function(arg1 : value1)
    Colon,
    /// function(arg1 VALUE value1)
    Value,
}

impl fmt::Display for FunctionArgOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionArgOperator::Equals => f.write_str("="),
            FunctionArgOperator::RightArrow => f.write_str("=>"),
            FunctionArgOperator::Assignment => f.write_str(":="),
            FunctionArgOperator::Colon => f.write_str(":"),
            FunctionArgOperator::Value => f.write_str("VALUE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FunctionArg {
    /// `name` is identifier
    ///
    /// Enabled when `Dialect::supports_named_fn_args_with_expr_name` returns 'false'
    Named {
        name: Ident,
        arg: FunctionArgExpr,
        operator: FunctionArgOperator,
    },
    /// `name` is arbitrary expression
    ///
    /// Enabled when `Dialect::supports_named_fn_args_with_expr_name` returns 'true'
    ExprNamed {
        name: Expr,
        arg: FunctionArgExpr,
        operator: FunctionArgOperator,
    },
    Unnamed(FunctionArgExpr),
}

impl fmt::Display for FunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionArg::Named {
                name,
                arg,
                operator,
            } => write!(f, "{name} {operator} {arg}"),
            FunctionArg::ExprNamed {
                name,
                arg,
                operator,
            } => write!(f, "{name} {operator} {arg}"),
            FunctionArg::Unnamed(unnamed_arg) => write!(f, "{unnamed_arg}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CloseCursor {
    All,
    Specific { name: Ident },
}

impl fmt::Display for CloseCursor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CloseCursor::All => write!(f, "ALL"),
            CloseCursor::Specific { name } => write!(f, "{name}"),
        }
    }
}

/// A Drop Domain statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropDomain {
    /// Whether to drop the domain if it exists
    pub if_exists: bool,
    /// The name of the domain to drop
    pub name: ObjectName,
    /// The behavior to apply when dropping the domain
    pub drop_behavior: Option<DropBehavior>,
}

/// A constant of form `<data_type> 'value'`.
/// This can represent ANSI SQL `DATE`, `TIME`, and `TIMESTAMP` literals (such as `DATE '2020-01-01'`),
/// as well as constants of other types (a non-standard PostgreSQL extension).
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TypedString {
    pub data_type: DataType,
    /// The value of the constant.
    /// Hint: you can unwrap the string value using `value.into_string()`.
    pub value: ValueWithSpan,
    /// Flags whether this TypedString uses the [ODBC syntax].
    ///
    /// Example:
    /// ```sql
    /// -- An ODBC date literal:
    /// SELECT {d '2025-07-16'}
    /// -- This is equivalent to the standard ANSI SQL literal:
    /// SELECT DATE '2025-07-16'
    ///
    /// [ODBC syntax]: https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/date-time-and-timestamp-literals?view=sql-server-2017
    pub uses_odbc_syntax: bool,
}

impl fmt::Display for TypedString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let data_type = &self.data_type;
        let value = &self.value;
        match self.uses_odbc_syntax {
            false => {
                write!(f, "{data_type}")?;
                write!(f, " {value}")
            }
            true => {
                let prefix = match data_type {
                    DataType::Date => "d",
                    DataType::Time(..) => "t",
                    DataType::Timestamp(..) => "ts",
                    _ => "?",
                };
                write!(f, "{{{prefix} {value}}}")
            }
        }
    }
}

/// A function call
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Function {
    pub name: ObjectName,
    /// Flags whether this function call uses the [ODBC syntax].
    ///
    /// Example:
    /// ```sql
    /// SELECT {fn CONCAT('foo', 'bar')}
    /// ```
    ///
    /// [ODBC syntax]: https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/scalar-function-calls?view=sql-server-2017
    pub uses_odbc_syntax: bool,
    /// The parameters to the function, including any options specified within the
    /// delimiting parentheses.
    ///
    /// Example:
    /// ```plaintext
    /// HISTOGRAM(0.5, 0.6)(x, y)
    /// ```
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/aggregate-functions/parametric-functions)
    pub parameters: FunctionArguments,
    /// The arguments to the function, including any options specified within the
    /// delimiting parentheses.
    pub args: FunctionArguments,
    /// e.g. `x > 5` in `COUNT(x) FILTER (WHERE x > 5)`
    pub filter: Option<Box<Expr>>,
    /// Indicates how `NULL`s should be handled in the calculation.
    ///
    /// Example:
    /// ```plaintext
    /// FIRST_VALUE( <expr> ) [ { IGNORE | RESPECT } NULLS ] OVER ...
    /// ```
    ///
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/functions/first_value)
    pub null_treatment: Option<NullTreatment>,
    /// The `OVER` clause, indicating a window function call.
    pub over: Option<WindowType>,
    /// A clause used with certain aggregate functions to control the ordering
    /// within grouped sets before the function is applied.
    ///
    /// Syntax:
    /// ```plaintext
    /// <aggregate_function>(expression) WITHIN GROUP (ORDER BY key [ASC | DESC], ...)
    /// ```
    pub within_group: Vec<OrderByExpr>,
}

impl fmt::Display for Function {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.uses_odbc_syntax {
            write!(f, "{{fn ")?;
        }

        write!(f, "{}{}{}", self.name, self.parameters, self.args)?;

        if !self.within_group.is_empty() {
            write!(
                f,
                " WITHIN GROUP (ORDER BY {})",
                display_comma_separated(&self.within_group)
            )?;
        }

        if let Some(filter_cond) = &self.filter {
            write!(f, " FILTER (WHERE {filter_cond})")?;
        }

        if let Some(null_treatment) = &self.null_treatment {
            write!(f, " {null_treatment}")?;
        }

        if let Some(o) = &self.over {
            f.write_str(" OVER ")?;
            o.fmt(f)?;
        }

        if self.uses_odbc_syntax {
            write!(f, "}}")?;
        }

        Ok(())
    }
}

/// The arguments passed to a function call.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FunctionArguments {
    /// Used for special functions like `CURRENT_TIMESTAMP` that are invoked
    /// without parentheses.
    None,
    /// On some dialects, a subquery can be passed without surrounding
    /// parentheses if it's the sole argument to the function.
    Subquery(Box<Query>),
    /// A normal function argument list, including any clauses within it such as
    /// `DISTINCT` or `ORDER BY`.
    List(FunctionArgumentList),
}

impl fmt::Display for FunctionArguments {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArguments::None => Ok(()),
            FunctionArguments::Subquery(query) => write!(f, "({query})"),
            FunctionArguments::List(args) => write!(f, "({args})"),
        }
    }
}

/// This represents everything inside the parentheses when calling a function.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct FunctionArgumentList {
    /// `[ ALL | DISTINCT ]`
    pub duplicate_treatment: Option<DuplicateTreatment>,
    /// The function arguments.
    pub args: Vec<FunctionArg>,
    /// Additional clauses specified within the argument list.
    pub clauses: Vec<FunctionArgumentClause>,
}

impl fmt::Display for FunctionArgumentList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(duplicate_treatment) = self.duplicate_treatment {
            write!(f, "{duplicate_treatment} ")?;
        }
        write!(f, "{}", display_comma_separated(&self.args))?;
        if !self.clauses.is_empty() {
            if !self.args.is_empty() {
                write!(f, " ")?;
            }
            write!(f, "{}", display_separated(&self.clauses, " "))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FunctionArgumentClause {
    /// Indicates how `NULL`s should be handled in the calculation, e.g. in `FIRST_VALUE` on [BigQuery].
    ///
    /// Syntax:
    /// ```plaintext
    /// { IGNORE | RESPECT } NULLS ]
    /// ```
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/navigation_functions#first_value
    IgnoreOrRespectNulls(NullTreatment),
    /// Specifies the the ordering for some ordered set aggregates, e.g. `ARRAY_AGG` on [BigQuery].
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#array_agg
    OrderBy(Vec<OrderByExpr>),
    /// Specifies a limit for the `ARRAY_AGG` and `ARRAY_CONCAT_AGG` functions on BigQuery.
    Limit(Expr),
    /// Specifies the behavior on overflow of the `LISTAGG` function.
    ///
    /// See <https://trino.io/docs/current/functions/aggregate.html>.
    OnOverflow(ListAggOnOverflow),
    /// Specifies a minimum or maximum bound on the input to [`ANY_VALUE`] on BigQuery.
    ///
    /// Syntax:
    /// ```plaintext
    /// HAVING { MAX | MIN } expression
    /// ```
    ///
    /// [`ANY_VALUE`]: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#any_value
    Having(HavingBound),
    /// The `SEPARATOR` clause to the [`GROUP_CONCAT`] function in MySQL.
    ///
    /// [`GROUP_CONCAT`]: https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html#function_group-concat
    Separator(Value),
    /// The `ON NULL` clause for some JSON functions.
    ///
    /// [MSSQL `JSON_ARRAY`](https://learn.microsoft.com/en-us/sql/t-sql/functions/json-array-transact-sql?view=sql-server-ver16)
    /// [MSSQL `JSON_OBJECT`](https://learn.microsoft.com/en-us/sql/t-sql/functions/json-object-transact-sql?view=sql-server-ver16>)
    /// [PostgreSQL JSON functions](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-JSON-PROCESSING)
    JsonNullClause(JsonNullClause),
    /// The `RETURNING` clause for some JSON functions in PostgreSQL
    ///
    /// [`JSON_OBJECT`](https://www.postgresql.org/docs/current/functions-json.html#:~:text=json_object)
    JsonReturningClause(JsonReturningClause),
}

impl fmt::Display for FunctionArgumentClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArgumentClause::IgnoreOrRespectNulls(null_treatment) => {
                write!(f, "{null_treatment}")
            }
            FunctionArgumentClause::OrderBy(order_by) => {
                write!(f, "ORDER BY {}", display_comma_separated(order_by))
            }
            FunctionArgumentClause::Limit(limit) => write!(f, "LIMIT {limit}"),
            FunctionArgumentClause::OnOverflow(on_overflow) => write!(f, "{on_overflow}"),
            FunctionArgumentClause::Having(bound) => write!(f, "{bound}"),
            FunctionArgumentClause::Separator(sep) => write!(f, "SEPARATOR {sep}"),
            FunctionArgumentClause::JsonNullClause(null_clause) => write!(f, "{null_clause}"),
            FunctionArgumentClause::JsonReturningClause(returning_clause) => {
                write!(f, "{returning_clause}")
            }
        }
    }
}

/// A method call
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Method {
    pub expr: Box<Expr>,
    // always non-empty
    pub method_chain: Vec<Function>,
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}.{}",
            self.expr,
            display_separated(&self.method_chain, ".")
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DuplicateTreatment {
    /// Perform the calculation only unique values.
    Distinct,
    /// Retain all duplicate values (the default).
    All,
}

impl fmt::Display for DuplicateTreatment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DuplicateTreatment::Distinct => write!(f, "DISTINCT"),
            DuplicateTreatment::All => write!(f, "ALL"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AnalyzeFormatKind {
    /// e.g. `EXPLAIN ANALYZE FORMAT JSON SELECT * FROM tbl`
    Keyword(AnalyzeFormat),
    /// e.g. `EXPLAIN ANALYZE FORMAT=JSON SELECT * FROM tbl`
    Assignment(AnalyzeFormat),
}

impl fmt::Display for AnalyzeFormatKind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            AnalyzeFormatKind::Keyword(format) => write!(f, "FORMAT {format}"),
            AnalyzeFormatKind::Assignment(format) => write!(f, "FORMAT={format}"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AnalyzeFormat {
    TEXT,
    GRAPHVIZ,
    JSON,
    TRADITIONAL,
    TREE,
}

impl fmt::Display for AnalyzeFormat {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            AnalyzeFormat::TEXT => "TEXT",
            AnalyzeFormat::GRAPHVIZ => "GRAPHVIZ",
            AnalyzeFormat::JSON => "JSON",
            AnalyzeFormat::TRADITIONAL => "TRADITIONAL",
            AnalyzeFormat::TREE => "TREE",
        })
    }
}

/// External table's available file format
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FileFormat {
    TEXTFILE,
    SEQUENCEFILE,
    ORC,
    PARQUET,
    AVRO,
    RCFILE,
    JSONFILE,
}

impl fmt::Display for FileFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::FileFormat::*;
        f.write_str(match self {
            TEXTFILE => "TEXTFILE",
            SEQUENCEFILE => "SEQUENCEFILE",
            ORC => "ORC",
            PARQUET => "PARQUET",
            AVRO => "AVRO",
            RCFILE => "RCFILE",
            JSONFILE => "JSONFILE",
        })
    }
}

/// The `ON OVERFLOW` clause of a LISTAGG invocation
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ListAggOnOverflow {
    /// `ON OVERFLOW ERROR`
    Error,

    /// `ON OVERFLOW TRUNCATE [ <filler> ] WITH[OUT] COUNT`
    Truncate {
        filler: Option<Box<Expr>>,
        with_count: bool,
    },
}

impl fmt::Display for ListAggOnOverflow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ON OVERFLOW")?;
        match self {
            ListAggOnOverflow::Error => write!(f, " ERROR"),
            ListAggOnOverflow::Truncate { filler, with_count } => {
                write!(f, " TRUNCATE")?;
                if let Some(filler) = filler {
                    write!(f, " {filler}")?;
                }
                if *with_count {
                    write!(f, " WITH")?;
                } else {
                    write!(f, " WITHOUT")?;
                }
                write!(f, " COUNT")
            }
        }
    }
}

/// The `HAVING` clause in a call to `ANY_VALUE` on BigQuery.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct HavingBound(pub HavingBoundKind, pub Expr);

impl fmt::Display for HavingBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HAVING {} {}", self.0, self.1)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum HavingBoundKind {
    Min,
    Max,
}

impl fmt::Display for HavingBoundKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HavingBoundKind::Min => write!(f, "MIN"),
            HavingBoundKind::Max => write!(f, "MAX"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ObjectType {
    Table,
    View,
    MaterializedView,
    Index,
    Schema,
    Database,
    Role,
    Sequence,
    Stage,
    Type,
    User,
    Stream,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::MaterializedView => "MATERIALIZED VIEW",
            ObjectType::Index => "INDEX",
            ObjectType::Schema => "SCHEMA",
            ObjectType::Database => "DATABASE",
            ObjectType::Role => "ROLE",
            ObjectType::Sequence => "SEQUENCE",
            ObjectType::Stage => "STAGE",
            ObjectType::Type => "TYPE",
            ObjectType::User => "USER",
            ObjectType::Stream => "STREAM",
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum KillType {
    Connection,
    Query,
    Mutation,
}

impl fmt::Display for KillType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            // MySQL
            KillType::Connection => "CONNECTION",
            KillType::Query => "QUERY",
            // Clickhouse supports Mutation
            KillType::Mutation => "MUTATION",
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum HiveDistributionStyle {
    PARTITIONED {
        columns: Vec<ColumnDef>,
    },
    SKEWED {
        columns: Vec<ColumnDef>,
        on: Vec<ColumnDef>,
        stored_as_directories: bool,
    },
    NONE,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum HiveRowFormat {
    SERDE { class: String },
    DELIMITED { delimiters: Vec<HiveRowDelimiter> },
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct HiveLoadDataFormat {
    pub serde: Expr,
    pub input_format: Expr,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct HiveRowDelimiter {
    pub delimiter: HiveDelimiter,
    pub char: Ident,
}

impl fmt::Display for HiveRowDelimiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.delimiter)?;
        write!(f, "{}", self.char)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum HiveDelimiter {
    FieldsTerminatedBy,
    FieldsEscapedBy,
    CollectionItemsTerminatedBy,
    MapKeysTerminatedBy,
    LinesTerminatedBy,
    NullDefinedAs,
}

impl fmt::Display for HiveDelimiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use HiveDelimiter::*;
        f.write_str(match self {
            FieldsTerminatedBy => "FIELDS TERMINATED BY",
            FieldsEscapedBy => "ESCAPED BY",
            CollectionItemsTerminatedBy => "COLLECTION ITEMS TERMINATED BY",
            MapKeysTerminatedBy => "MAP KEYS TERMINATED BY",
            LinesTerminatedBy => "LINES TERMINATED BY",
            NullDefinedAs => "NULL DEFINED AS",
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum HiveDescribeFormat {
    Extended,
    Formatted,
}

impl fmt::Display for HiveDescribeFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use HiveDescribeFormat::*;
        f.write_str(match self {
            Extended => "EXTENDED",
            Formatted => "FORMATTED",
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DescribeAlias {
    Describe,
    Explain,
    Desc,
}

impl fmt::Display for DescribeAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use DescribeAlias::*;
        f.write_str(match self {
            Describe => "DESCRIBE",
            Explain => "EXPLAIN",
            Desc => "DESC",
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[allow(clippy::large_enum_variant)]
pub enum HiveIOFormat {
    IOF {
        input_format: Expr,
        output_format: Expr,
    },
    FileFormat {
        format: FileFormat,
    },
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct HiveFormat {
    pub row_format: Option<HiveRowFormat>,
    pub serde_properties: Option<Vec<SqlOption>>,
    pub storage: Option<HiveIOFormat>,
    pub location: Option<String>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ClusteredIndex {
    pub name: Ident,
    pub asc: Option<bool>,
}

impl fmt::Display for ClusteredIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        match self.asc {
            Some(true) => write!(f, " ASC"),
            Some(false) => write!(f, " DESC"),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableOptionsClustered {
    ColumnstoreIndex,
    ColumnstoreIndexOrder(Vec<Ident>),
    Index(Vec<ClusteredIndex>),
}

impl fmt::Display for TableOptionsClustered {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableOptionsClustered::ColumnstoreIndex => {
                write!(f, "CLUSTERED COLUMNSTORE INDEX")
            }
            TableOptionsClustered::ColumnstoreIndexOrder(values) => {
                write!(
                    f,
                    "CLUSTERED COLUMNSTORE INDEX ORDER ({})",
                    display_comma_separated(values)
                )
            }
            TableOptionsClustered::Index(values) => {
                write!(f, "CLUSTERED INDEX ({})", display_comma_separated(values))
            }
        }
    }
}

/// Specifies which partition the boundary values on table partitioning belongs to.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum PartitionRangeDirection {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SqlOption {
    /// Clustered represents the clustered version of table storage for MSSQL.
    ///
    /// <https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse?view=aps-pdw-2016-au7#TableOptions>
    Clustered(TableOptionsClustered),
    /// Single identifier options, e.g. `HEAP` for MSSQL.
    ///
    /// <https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse?view=aps-pdw-2016-au7#TableOptions>
    Ident(Ident),
    /// Any option that consists of a key value pair where the value is an expression. e.g.
    ///
    ///   WITH(DISTRIBUTION = ROUND_ROBIN)
    KeyValue { key: Ident, value: Expr },
    /// One or more table partitions and represents which partition the boundary values belong to,
    /// e.g.
    ///
    ///   PARTITION (id RANGE LEFT FOR VALUES (10, 20, 30, 40))
    ///
    /// <https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse?view=aps-pdw-2016-au7#TablePartitionOptions>
    Partition {
        column_name: Ident,
        range_direction: Option<PartitionRangeDirection>,
        for_values: Vec<Expr>,
    },
    /// Comment parameter (supports `=` and no `=` syntax)
    Comment(CommentDef),
    /// MySQL TableSpace option
    /// <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
    TableSpace(TablespaceOption),
    /// An option representing a key value pair, where the value is a parenthesized list and with an optional name
    /// e.g.
    ///
    ///   UNION  = (tbl_name\[,tbl_name\]...) <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
    ///   ENGINE = ReplicatedMergeTree('/table_name','{replica}', ver) <https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication>
    ///   ENGINE = SummingMergeTree(\[columns\]) <https://clickhouse.com/docs/engines/table-engines/mergetree-family/summingmergetree>
    NamedParenthesizedList(NamedParenthesizedList),
}

impl fmt::Display for SqlOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SqlOption::Clustered(c) => write!(f, "{c}"),
            SqlOption::Ident(ident) => {
                write!(f, "{ident}")
            }
            SqlOption::KeyValue { key: name, value } => {
                write!(f, "{name} = {value}")
            }
            SqlOption::Partition {
                column_name,
                range_direction,
                for_values,
            } => {
                let direction = match range_direction {
                    Some(PartitionRangeDirection::Left) => " LEFT",
                    Some(PartitionRangeDirection::Right) => " RIGHT",
                    None => "",
                };

                write!(
                    f,
                    "PARTITION ({} RANGE{} FOR VALUES ({}))",
                    column_name,
                    direction,
                    display_comma_separated(for_values)
                )
            }
            SqlOption::TableSpace(tablespace_option) => {
                write!(f, "TABLESPACE {}", tablespace_option.name)?;
                match tablespace_option.storage {
                    Some(StorageType::Disk) => write!(f, " STORAGE DISK"),
                    Some(StorageType::Memory) => write!(f, " STORAGE MEMORY"),
                    None => Ok(()),
                }
            }
            SqlOption::Comment(comment) => match comment {
                CommentDef::WithEq(comment) => {
                    write!(f, "COMMENT = '{comment}'")
                }
                CommentDef::WithoutEq(comment) => {
                    write!(f, "COMMENT '{comment}'")
                }
            },
            SqlOption::NamedParenthesizedList(value) => {
                write!(f, "{} = ", value.key)?;
                if let Some(key) = &value.name {
                    write!(f, "{key}")?;
                }
                if !value.values.is_empty() {
                    write!(f, "({})", display_comma_separated(&value.values))?
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum StorageType {
    Disk,
    Memory,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// MySql TableSpace option
/// <https://dev.mysql.com/doc/refman/8.4/en/create-table.html>
pub struct TablespaceOption {
    pub name: String,
    pub storage: Option<StorageType>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SecretOption {
    pub key: Ident,
    pub value: Ident,
}

impl fmt::Display for SecretOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.key, self.value)
    }
}

/// A `CREATE SERVER` statement.
///
/// [PostgreSQL Documentation](https://www.postgresql.org/docs/current/sql-createserver.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateServerStatement {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub server_type: Option<Ident>,
    pub version: Option<Ident>,
    pub foreign_data_wrapper: ObjectName,
    pub options: Option<Vec<CreateServerOption>>,
}

impl fmt::Display for CreateServerStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let CreateServerStatement {
            name,
            if_not_exists,
            server_type,
            version,
            foreign_data_wrapper,
            options,
        } = self;

        write!(
            f,
            "CREATE SERVER {if_not_exists}{name} ",
            if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
        )?;

        if let Some(st) = server_type {
            write!(f, "TYPE {st} ")?;
        }

        if let Some(v) = version {
            write!(f, "VERSION {v} ")?;
        }

        write!(f, "FOREIGN DATA WRAPPER {foreign_data_wrapper}")?;

        if let Some(o) = options {
            write!(f, " OPTIONS ({o})", o = display_comma_separated(o))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateServerOption {
    pub key: Ident,
    pub value: Ident,
}

impl fmt::Display for CreateServerOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.key, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AttachDuckDBDatabaseOption {
    ReadOnly(Option<bool>),
    Type(Ident),
}

impl fmt::Display for AttachDuckDBDatabaseOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AttachDuckDBDatabaseOption::ReadOnly(Some(true)) => write!(f, "READ_ONLY true"),
            AttachDuckDBDatabaseOption::ReadOnly(Some(false)) => write!(f, "READ_ONLY false"),
            AttachDuckDBDatabaseOption::ReadOnly(None) => write!(f, "READ_ONLY"),
            AttachDuckDBDatabaseOption::Type(t) => write!(f, "TYPE {t}"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TransactionMode {
    AccessMode(TransactionAccessMode),
    IsolationLevel(TransactionIsolationLevel),
}

impl fmt::Display for TransactionMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => write!(f, "{access_mode}"),
            IsolationLevel(iso_level) => write!(f, "ISOLATION LEVEL {iso_level}"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

impl fmt::Display for TransactionAccessMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionAccessMode::*;
        f.write_str(match self {
            ReadOnly => "READ ONLY",
            ReadWrite => "READ WRITE",
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

impl fmt::Display for TransactionIsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionIsolationLevel::*;
        f.write_str(match self {
            ReadUncommitted => "READ UNCOMMITTED",
            ReadCommitted => "READ COMMITTED",
            RepeatableRead => "REPEATABLE READ",
            Serializable => "SERIALIZABLE",
            Snapshot => "SNAPSHOT",
        })
    }
}

/// Modifier for the transaction in the `BEGIN` syntax
///
/// SQLite: <https://sqlite.org/lang_transaction.html>
/// MS-SQL: <https://learn.microsoft.com/en-us/sql/t-sql/language-elements/try-catch-transact-sql>
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TransactionModifier {
    Deferred,
    Immediate,
    Exclusive,
    Try,
    Catch,
}

impl fmt::Display for TransactionModifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionModifier::*;
        f.write_str(match self {
            Deferred => "DEFERRED",
            Immediate => "IMMEDIATE",
            Exclusive => "EXCLUSIVE",
            Try => "TRY",
            Catch => "CATCH",
        })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ShowStatementFilter {
    Like(String),
    ILike(String),
    Where(Expr),
    NoKeyword(String),
}

impl fmt::Display for ShowStatementFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => write!(f, "LIKE '{}'", value::escape_single_quote_string(pattern)),
            ILike(pattern) => write!(f, "ILIKE {}", value::escape_single_quote_string(pattern)),
            Where(expr) => write!(f, "WHERE {expr}"),
            NoKeyword(pattern) => write!(f, "'{}'", value::escape_single_quote_string(pattern)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ShowStatementInClause {
    IN,
    FROM,
}

impl fmt::Display for ShowStatementInClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ShowStatementInClause::*;
        match self {
            FROM => write!(f, "FROM"),
            IN => write!(f, "IN"),
        }
    }
}

/// Sqlite specific syntax
///
/// See [Sqlite documentation](https://sqlite.org/lang_conflict.html)
/// for more details.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SqliteOnConflict {
    Rollback,
    Abort,
    Fail,
    Ignore,
    Replace,
}

impl fmt::Display for SqliteOnConflict {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SqliteOnConflict::*;
        match self {
            Rollback => write!(f, "OR ROLLBACK"),
            Abort => write!(f, "OR ABORT"),
            Fail => write!(f, "OR FAIL"),
            Ignore => write!(f, "OR IGNORE"),
            Replace => write!(f, "OR REPLACE"),
        }
    }
}

/// Mysql specific syntax
///
/// See [Mysql documentation](https://dev.mysql.com/doc/refman/8.0/en/replace.html)
/// See [Mysql documentation](https://dev.mysql.com/doc/refman/8.0/en/insert.html)
/// for more details.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MysqlInsertPriority {
    LowPriority,
    Delayed,
    HighPriority,
}

impl fmt::Display for crate::ast::MysqlInsertPriority {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use MysqlInsertPriority::*;
        match self {
            LowPriority => write!(f, "LOW_PRIORITY"),
            Delayed => write!(f, "DELAYED"),
            HighPriority => write!(f, "HIGH_PRIORITY"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CopySource {
    Table {
        /// The name of the table to copy from.
        table_name: ObjectName,
        /// A list of column names to copy. Empty list means that all columns
        /// are copied.
        columns: Vec<Ident>,
    },
    Query(Box<Query>),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CopyTarget {
    Stdin,
    Stdout,
    File {
        /// The path name of the input or output file.
        filename: String,
    },
    Program {
        /// A command to execute
        command: String,
    },
}

impl fmt::Display for CopyTarget {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CopyTarget::*;
        match self {
            Stdin => write!(f, "STDIN"),
            Stdout => write!(f, "STDOUT"),
            File { filename } => write!(f, "'{}'", value::escape_single_quote_string(filename)),
            Program { command } => write!(
                f,
                "PROGRAM '{}'",
                value::escape_single_quote_string(command)
            ),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum OnCommit {
    DeleteRows,
    PreserveRows,
    Drop,
}

/// An option in `COPY` statement.
///
/// <https://www.postgresql.org/docs/14/sql-copy.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CopyOption {
    /// FORMAT format_name
    Format(Ident),
    /// FREEZE \[ boolean \]
    Freeze(bool),
    /// DELIMITER 'delimiter_character'
    Delimiter(char),
    /// NULL 'null_string'
    Null(String),
    /// HEADER \[ boolean \]
    Header(bool),
    /// QUOTE 'quote_character'
    Quote(char),
    /// ESCAPE 'escape_character'
    Escape(char),
    /// FORCE_QUOTE { ( column_name [, ...] ) | * }
    ForceQuote(Vec<Ident>),
    /// FORCE_NOT_NULL ( column_name [, ...] )
    ForceNotNull(Vec<Ident>),
    /// FORCE_NULL ( column_name [, ...] )
    ForceNull(Vec<Ident>),
    /// ENCODING 'encoding_name'
    Encoding(String),
}

impl fmt::Display for CopyOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CopyOption::*;
        match self {
            Format(name) => write!(f, "FORMAT {name}"),
            Freeze(true) => write!(f, "FREEZE"),
            Freeze(false) => write!(f, "FREEZE FALSE"),
            Delimiter(char) => write!(f, "DELIMITER '{char}'"),
            Null(string) => write!(f, "NULL '{}'", value::escape_single_quote_string(string)),
            Header(true) => write!(f, "HEADER"),
            Header(false) => write!(f, "HEADER FALSE"),
            Quote(char) => write!(f, "QUOTE '{char}'"),
            Escape(char) => write!(f, "ESCAPE '{char}'"),
            ForceQuote(columns) => write!(f, "FORCE_QUOTE ({})", display_comma_separated(columns)),
            ForceNotNull(columns) => {
                write!(f, "FORCE_NOT_NULL ({})", display_comma_separated(columns))
            }
            ForceNull(columns) => write!(f, "FORCE_NULL ({})", display_comma_separated(columns)),
            Encoding(name) => write!(f, "ENCODING '{}'", value::escape_single_quote_string(name)),
        }
    }
}

/// An option in `COPY` statement before PostgreSQL version 9.0.
///
/// [PostgreSQL](https://www.postgresql.org/docs/8.4/sql-copy.html)
/// [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY-alphabetical-parm-list.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CopyLegacyOption {
    /// ACCEPTANYDATE
    AcceptAnyDate,
    /// ACCEPTINVCHARS
    AcceptInvChars(Option<String>),
    /// ADDQUOTES
    AddQuotes,
    /// ALLOWOVERWRITE
    AllowOverwrite,
    /// BINARY
    Binary,
    /// BLANKSASNULL
    BlankAsNull,
    /// BZIP2
    Bzip2,
    /// CLEANPATH
    CleanPath,
    /// COMPUPDATE [ PRESET | { ON | TRUE } | { OFF | FALSE } ]
    CompUpdate { preset: bool, enabled: Option<bool> },
    /// CSV ...
    Csv(Vec<CopyLegacyCsvOption>),
    /// DATEFORMAT \[ AS \] {'dateformat_string' | 'auto' }
    DateFormat(Option<String>),
    /// DELIMITER \[ AS \] 'delimiter_character'
    Delimiter(char),
    /// EMPTYASNULL
    EmptyAsNull,
    /// ENCRYPTED \[ AUTO \]
    Encrypted { auto: bool },
    /// ESCAPE
    Escape,
    /// EXTENSION 'extension-name'
    Extension(String),
    /// FIXEDWIDTH \[ AS \] 'fixedwidth-spec'
    FixedWidth(String),
    /// GZIP
    Gzip,
    /// HEADER
    Header,
    /// IAM_ROLE { DEFAULT | 'arn:aws:iam::123456789:role/role1' }
    IamRole(IamRoleKind),
    /// IGNOREHEADER \[ AS \] number_rows
    IgnoreHeader(u64),
    /// JSON
    Json,
    /// MANIFEST \[ VERBOSE \]
    Manifest { verbose: bool },
    /// MAXFILESIZE \[ AS \] max-size \[ MB | GB \]
    MaxFileSize(FileSize),
    /// NULL \[ AS \] 'null_string'
    Null(String),
    /// PARALLEL [ { ON | TRUE } | { OFF | FALSE } ]
    Parallel(Option<bool>),
    /// PARQUET
    Parquet,
    /// PARTITION BY ( column_name [, ... ] ) \[ INCLUDE \]
    PartitionBy(UnloadPartitionBy),
    /// REGION \[ AS \] 'aws-region' }
    Region(String),
    /// REMOVEQUOTES
    RemoveQuotes,
    /// ROWGROUPSIZE \[ AS \] size \[ MB | GB \]
    RowGroupSize(FileSize),
    /// STATUPDATE [ { ON | TRUE } | { OFF | FALSE } ]
    StatUpdate(Option<bool>),
    /// TIMEFORMAT \[ AS \] {'timeformat_string' | 'auto' | 'epochsecs' | 'epochmillisecs' }
    TimeFormat(Option<String>),
    /// TRUNCATECOLUMNS
    TruncateColumns,
    /// ZSTD
    Zstd,
}

impl fmt::Display for CopyLegacyOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CopyLegacyOption::*;
        match self {
            AcceptAnyDate => write!(f, "ACCEPTANYDATE"),
            AcceptInvChars(ch) => {
                write!(f, "ACCEPTINVCHARS")?;
                if let Some(ch) = ch {
                    write!(f, " '{}'", value::escape_single_quote_string(ch))?;
                }
                Ok(())
            }
            AddQuotes => write!(f, "ADDQUOTES"),
            AllowOverwrite => write!(f, "ALLOWOVERWRITE"),
            Binary => write!(f, "BINARY"),
            BlankAsNull => write!(f, "BLANKSASNULL"),
            Bzip2 => write!(f, "BZIP2"),
            CleanPath => write!(f, "CLEANPATH"),
            CompUpdate { preset, enabled } => {
                write!(f, "COMPUPDATE")?;
                if *preset {
                    write!(f, " PRESET")?;
                } else if let Some(enabled) = enabled {
                    write!(
                        f,
                        "{}",
                        match enabled {
                            true => " TRUE",
                            false => " FALSE",
                        }
                    )?;
                }
                Ok(())
            }
            Csv(opts) => {
                write!(f, "CSV")?;
                if !opts.is_empty() {
                    write!(f, " {}", display_separated(opts, " "))?;
                }
                Ok(())
            }
            DateFormat(fmt) => {
                write!(f, "DATEFORMAT")?;
                if let Some(fmt) = fmt {
                    write!(f, " '{}'", value::escape_single_quote_string(fmt))?;
                }
                Ok(())
            }
            Delimiter(char) => write!(f, "DELIMITER '{char}'"),
            EmptyAsNull => write!(f, "EMPTYASNULL"),
            Encrypted { auto } => write!(f, "ENCRYPTED{}", if *auto { " AUTO" } else { "" }),
            Escape => write!(f, "ESCAPE"),
            Extension(ext) => write!(f, "EXTENSION '{}'", value::escape_single_quote_string(ext)),
            FixedWidth(spec) => write!(
                f,
                "FIXEDWIDTH '{}'",
                value::escape_single_quote_string(spec)
            ),
            Gzip => write!(f, "GZIP"),
            Header => write!(f, "HEADER"),
            IamRole(role) => write!(f, "IAM_ROLE {role}"),
            IgnoreHeader(num_rows) => write!(f, "IGNOREHEADER {num_rows}"),
            Json => write!(f, "JSON"),
            Manifest { verbose } => write!(f, "MANIFEST{}", if *verbose { " VERBOSE" } else { "" }),
            MaxFileSize(file_size) => write!(f, "MAXFILESIZE {file_size}"),
            Null(string) => write!(f, "NULL '{}'", value::escape_single_quote_string(string)),
            Parallel(enabled) => {
                write!(
                    f,
                    "PARALLEL{}",
                    match enabled {
                        Some(true) => " TRUE",
                        Some(false) => " FALSE",
                        _ => "",
                    }
                )
            }
            Parquet => write!(f, "PARQUET"),
            PartitionBy(p) => write!(f, "{p}"),
            Region(region) => write!(f, "REGION '{}'", value::escape_single_quote_string(region)),
            RemoveQuotes => write!(f, "REMOVEQUOTES"),
            RowGroupSize(file_size) => write!(f, "ROWGROUPSIZE {file_size}"),
            StatUpdate(enabled) => {
                write!(
                    f,
                    "STATUPDATE{}",
                    match enabled {
                        Some(true) => " TRUE",
                        Some(false) => " FALSE",
                        _ => "",
                    }
                )
            }
            TimeFormat(fmt) => {
                write!(f, "TIMEFORMAT")?;
                if let Some(fmt) = fmt {
                    write!(f, " '{}'", value::escape_single_quote_string(fmt))?;
                }
                Ok(())
            }
            TruncateColumns => write!(f, "TRUNCATECOLUMNS"),
            Zstd => write!(f, "ZSTD"),
        }
    }
}

/// ```sql
/// SIZE \[ MB | GB \]
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct FileSize {
    pub size: Value,
    pub unit: Option<FileSizeUnit>,
}

impl fmt::Display for FileSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.size)?;
        if let Some(unit) = &self.unit {
            write!(f, " {unit}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FileSizeUnit {
    MB,
    GB,
}

impl fmt::Display for FileSizeUnit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FileSizeUnit::MB => write!(f, "MB"),
            FileSizeUnit::GB => write!(f, "GB"),
        }
    }
}

/// Specifies the partition keys for the unload operation
///
/// ```sql
/// PARTITION BY ( column_name [, ... ] ) [ INCLUDE ]
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct UnloadPartitionBy {
    pub columns: Vec<Ident>,
    pub include: bool,
}

impl fmt::Display for UnloadPartitionBy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PARTITION BY ({}){}",
            display_comma_separated(&self.columns),
            if self.include { " INCLUDE" } else { "" }
        )
    }
}

/// An `IAM_ROLE` option in the AWS ecosystem
///
/// [Redshift COPY](https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html#copy-iam-role)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum IamRoleKind {
    /// Default role
    Default,
    /// Specific role ARN, for example: `arn:aws:iam::123456789:role/role1`
    Arn(String),
}

impl fmt::Display for IamRoleKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IamRoleKind::Default => write!(f, "DEFAULT"),
            IamRoleKind::Arn(arn) => write!(f, "'{arn}'"),
        }
    }
}

/// A `CSV` option in `COPY` statement before PostgreSQL version 9.0.
///
/// <https://www.postgresql.org/docs/8.4/sql-copy.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CopyLegacyCsvOption {
    /// HEADER
    Header,
    /// QUOTE \[ AS \] 'quote_character'
    Quote(char),
    /// ESCAPE \[ AS \] 'escape_character'
    Escape(char),
    /// FORCE QUOTE { column_name [, ...] | * }
    ForceQuote(Vec<Ident>),
    /// FORCE NOT NULL column_name [, ...]
    ForceNotNull(Vec<Ident>),
}

impl fmt::Display for CopyLegacyCsvOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CopyLegacyCsvOption::*;
        match self {
            Header => write!(f, "HEADER"),
            Quote(char) => write!(f, "QUOTE '{char}'"),
            Escape(char) => write!(f, "ESCAPE '{char}'"),
            ForceQuote(columns) => write!(f, "FORCE QUOTE {}", display_comma_separated(columns)),
            ForceNotNull(columns) => {
                write!(f, "FORCE NOT NULL {}", display_comma_separated(columns))
            }
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum DiscardObject {
    ALL,
    PLANS,
    SEQUENCES,
    TEMP,
}

impl fmt::Display for DiscardObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DiscardObject::ALL => f.write_str("ALL"),
            DiscardObject::PLANS => f.write_str("PLANS"),
            DiscardObject::SEQUENCES => f.write_str("SEQUENCES"),
            DiscardObject::TEMP => f.write_str("TEMP"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FlushType {
    BinaryLogs,
    EngineLogs,
    ErrorLogs,
    GeneralLogs,
    Hosts,
    Logs,
    Privileges,
    OptimizerCosts,
    RelayLogs,
    SlowLogs,
    Status,
    UserResources,
    Tables,
}

impl fmt::Display for FlushType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FlushType::BinaryLogs => f.write_str("BINARY LOGS"),
            FlushType::EngineLogs => f.write_str("ENGINE LOGS"),
            FlushType::ErrorLogs => f.write_str("ERROR LOGS"),
            FlushType::GeneralLogs => f.write_str("GENERAL LOGS"),
            FlushType::Hosts => f.write_str("HOSTS"),
            FlushType::Logs => f.write_str("LOGS"),
            FlushType::Privileges => f.write_str("PRIVILEGES"),
            FlushType::OptimizerCosts => f.write_str("OPTIMIZER_COSTS"),
            FlushType::RelayLogs => f.write_str("RELAY LOGS"),
            FlushType::SlowLogs => f.write_str("SLOW LOGS"),
            FlushType::Status => f.write_str("STATUS"),
            FlushType::UserResources => f.write_str("USER_RESOURCES"),
            FlushType::Tables => f.write_str("TABLES"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FlushLocation {
    NoWriteToBinlog,
    Local,
}

impl fmt::Display for FlushLocation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FlushLocation::NoWriteToBinlog => f.write_str("NO_WRITE_TO_BINLOG"),
            FlushLocation::Local => f.write_str("LOCAL"),
        }
    }
}

/// Optional context modifier for statements that can be or `LOCAL`, `GLOBAL`, or `SESSION`.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ContextModifier {
    /// `LOCAL` identifier, usually related to transactional states.
    Local,
    /// `SESSION` identifier
    Session,
    /// `GLOBAL` identifier
    Global,
}

impl fmt::Display for ContextModifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Local => {
                write!(f, "LOCAL ")
            }
            Self::Session => {
                write!(f, "SESSION ")
            }
            Self::Global => {
                write!(f, "GLOBAL ")
            }
        }
    }
}

/// Function describe in DROP FUNCTION.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DropFunctionOption {
    Restrict,
    Cascade,
}

impl fmt::Display for DropFunctionOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DropFunctionOption::Restrict => write!(f, "RESTRICT "),
            DropFunctionOption::Cascade => write!(f, "CASCADE  "),
        }
    }
}

/// Generic function description for DROP FUNCTION and CREATE TRIGGER.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct FunctionDesc {
    pub name: ObjectName,
    pub args: Option<Vec<OperateFunctionArg>>,
}

impl fmt::Display for FunctionDesc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(args) = &self.args {
            write!(f, "({})", display_comma_separated(args))?;
        }
        Ok(())
    }
}

/// Function argument in CREATE OR DROP FUNCTION.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OperateFunctionArg {
    pub mode: Option<ArgMode>,
    pub name: Option<Ident>,
    pub data_type: DataType,
    pub default_expr: Option<Expr>,
}

impl OperateFunctionArg {
    /// Returns an unnamed argument.
    pub fn unnamed(data_type: DataType) -> Self {
        Self {
            mode: None,
            name: None,
            data_type,
            default_expr: None,
        }
    }

    /// Returns an argument with name.
    pub fn with_name(name: &str, data_type: DataType) -> Self {
        Self {
            mode: None,
            name: Some(name.into()),
            data_type,
            default_expr: None,
        }
    }
}

impl fmt::Display for OperateFunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(mode) = &self.mode {
            write!(f, "{mode} ")?;
        }
        if let Some(name) = &self.name {
            write!(f, "{name} ")?;
        }
        write!(f, "{}", self.data_type)?;
        if let Some(default_expr) = &self.default_expr {
            write!(f, " = {default_expr}")?;
        }
        Ok(())
    }
}

/// The mode of an argument in CREATE FUNCTION.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ArgMode {
    In,
    Out,
    InOut,
}

impl fmt::Display for ArgMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ArgMode::In => write!(f, "IN"),
            ArgMode::Out => write!(f, "OUT"),
            ArgMode::InOut => write!(f, "INOUT"),
        }
    }
}

/// These attributes inform the query optimizer about the behavior of the function.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FunctionBehavior {
    Immutable,
    Stable,
    Volatile,
}

impl fmt::Display for FunctionBehavior {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionBehavior::Immutable => write!(f, "IMMUTABLE"),
            FunctionBehavior::Stable => write!(f, "STABLE"),
            FunctionBehavior::Volatile => write!(f, "VOLATILE"),
        }
    }
}

/// These attributes describe the behavior of the function when called with a null argument.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FunctionCalledOnNull {
    CalledOnNullInput,
    ReturnsNullOnNullInput,
    Strict,
}

impl fmt::Display for FunctionCalledOnNull {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionCalledOnNull::CalledOnNullInput => write!(f, "CALLED ON NULL INPUT"),
            FunctionCalledOnNull::ReturnsNullOnNullInput => write!(f, "RETURNS NULL ON NULL INPUT"),
            FunctionCalledOnNull::Strict => write!(f, "STRICT"),
        }
    }
}

/// If it is safe for PostgreSQL to call the function from multiple threads at once
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FunctionParallel {
    Unsafe,
    Restricted,
    Safe,
}

impl fmt::Display for FunctionParallel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionParallel::Unsafe => write!(f, "PARALLEL UNSAFE"),
            FunctionParallel::Restricted => write!(f, "PARALLEL RESTRICTED"),
            FunctionParallel::Safe => write!(f, "PARALLEL SAFE"),
        }
    }
}

/// [BigQuery] Determinism specifier used in a UDF definition.
///
/// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#syntax_11
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FunctionDeterminismSpecifier {
    Deterministic,
    NotDeterministic,
}

impl fmt::Display for FunctionDeterminismSpecifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionDeterminismSpecifier::Deterministic => {
                write!(f, "DETERMINISTIC")
            }
            FunctionDeterminismSpecifier::NotDeterministic => {
                write!(f, "NOT DETERMINISTIC")
            }
        }
    }
}

/// Represent the expression body of a `CREATE FUNCTION` statement as well as
/// where within the statement, the body shows up.
///
/// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#syntax_11
/// [PostgreSQL]: https://www.postgresql.org/docs/15/sql-createfunction.html
/// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CreateFunctionBody {
    /// A function body expression using the 'AS' keyword and shows up
    /// before any `OPTIONS` clause.
    ///
    /// Example:
    /// ```sql
    /// CREATE FUNCTION myfunc(x FLOAT64, y FLOAT64) RETURNS FLOAT64
    /// AS (x * y)
    /// OPTIONS(description="desc");
    /// ```
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#syntax_11
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/sql-createfunction.html
    AsBeforeOptions {
        /// The primary expression.
        body: Expr,
        /// Link symbol if the primary expression contains the name of shared library file.
        ///
        /// Example:
        /// ```sql
        /// CREATE FUNCTION cas_in(input cstring) RETURNS cas
        /// AS 'MODULE_PATHNAME', 'cas_in_wrapper'
        /// ```
        /// [PostgreSQL]: https://www.postgresql.org/docs/current/sql-createfunction.html
        link_symbol: Option<Expr>,
    },
    /// A function body expression using the 'AS' keyword and shows up
    /// after any `OPTIONS` clause.
    ///
    /// Example:
    /// ```sql
    /// CREATE FUNCTION myfunc(x FLOAT64, y FLOAT64) RETURNS FLOAT64
    /// OPTIONS(description="desc")
    /// AS (x * y);
    /// ```
    ///
    /// [BigQuery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#syntax_11
    AsAfterOptions(Expr),
    /// Function body with statements before the `RETURN` keyword.
    ///
    /// Example:
    /// ```sql
    /// CREATE FUNCTION my_scalar_udf(a INT, b INT)
    /// RETURNS INT
    /// AS
    /// BEGIN
    ///     DECLARE c INT;
    ///     SET c = a + b;
    ///     RETURN c;
    /// END
    /// ```
    ///
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql
    AsBeginEnd(BeginEndStatements),
    /// Function body expression using the 'RETURN' keyword.
    ///
    /// Example:
    /// ```sql
    /// CREATE FUNCTION myfunc(a INTEGER, IN b INTEGER = 1) RETURNS INTEGER
    /// LANGUAGE SQL
    /// RETURN a + b;
    /// ```
    ///
    /// [PostgreSQL]: https://www.postgresql.org/docs/current/sql-createfunction.html
    Return(Expr),

    /// Function body expression using the 'AS RETURN' keywords
    ///
    /// Example:
    /// ```sql
    /// CREATE FUNCTION myfunc(a INT, b INT)
    /// RETURNS TABLE
    /// AS RETURN (SELECT a + b AS sum);
    /// ```
    ///
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql
    AsReturnExpr(Expr),

    /// Function body expression using the 'AS RETURN' keywords, with an un-parenthesized SELECT query
    ///
    /// Example:
    /// ```sql
    /// CREATE FUNCTION myfunc(a INT, b INT)
    /// RETURNS TABLE
    /// AS RETURN SELECT a + b AS sum;
    /// ```
    ///
    /// [MsSql]: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql?view=sql-server-ver16#select_stmt
    AsReturnSelect(Select),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CreateFunctionUsing {
    Jar(String),
    File(String),
    Archive(String),
}

impl fmt::Display for CreateFunctionUsing {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "USING ")?;
        match self {
            CreateFunctionUsing::Jar(uri) => write!(f, "JAR '{uri}'"),
            CreateFunctionUsing::File(uri) => write!(f, "FILE '{uri}'"),
            CreateFunctionUsing::Archive(uri) => write!(f, "ARCHIVE '{uri}'"),
        }
    }
}

/// `NAME = <EXPR>` arguments for DuckDB macros
///
/// See [Create Macro - DuckDB](https://duckdb.org/docs/sql/statements/create_macro)
/// for more details
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MacroArg {
    pub name: Ident,
    pub default_expr: Option<Expr>,
}

impl MacroArg {
    /// Returns an argument with name.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            default_expr: None,
        }
    }
}

impl fmt::Display for MacroArg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(default_expr) = &self.default_expr {
            write!(f, " := {default_expr}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MacroDefinition {
    Expr(Expr),
    Table(Box<Query>),
}

impl fmt::Display for MacroDefinition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MacroDefinition::Expr(expr) => write!(f, "{expr}")?,
            MacroDefinition::Table(query) => write!(f, "{query}")?,
        }
        Ok(())
    }
}

/// Schema possible naming variants ([1]).
///
/// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#schema-definition
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SchemaName {
    /// Only schema name specified: `<schema name>`.
    Simple(ObjectName),
    /// Only authorization identifier specified: `AUTHORIZATION <schema authorization identifier>`.
    UnnamedAuthorization(Ident),
    /// Both schema name and authorization identifier specified: `<schema name>  AUTHORIZATION <schema authorization identifier>`.
    NamedAuthorization(ObjectName, Ident),
}

impl fmt::Display for SchemaName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaName::Simple(name) => {
                write!(f, "{name}")
            }
            SchemaName::UnnamedAuthorization(authorization) => {
                write!(f, "AUTHORIZATION {authorization}")
            }
            SchemaName::NamedAuthorization(name, authorization) => {
                write!(f, "{name} AUTHORIZATION {authorization}")
            }
        }
    }
}

/// Fulltext search modifiers ([1]).
///
/// [1]: https://dev.mysql.com/doc/refman/8.0/en/fulltext-search.html#function_match
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SearchModifier {
    /// `IN NATURAL LANGUAGE MODE`.
    InNaturalLanguageMode,
    /// `IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION`.
    InNaturalLanguageModeWithQueryExpansion,
    ///`IN BOOLEAN MODE`.
    InBooleanMode,
    ///`WITH QUERY EXPANSION`.
    WithQueryExpansion,
}

impl fmt::Display for SearchModifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InNaturalLanguageMode => {
                write!(f, "IN NATURAL LANGUAGE MODE")?;
            }
            Self::InNaturalLanguageModeWithQueryExpansion => {
                write!(f, "IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION")?;
            }
            Self::InBooleanMode => {
                write!(f, "IN BOOLEAN MODE")?;
            }
            Self::WithQueryExpansion => {
                write!(f, "WITH QUERY EXPANSION")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct LockTable {
    pub table: Ident,
    pub alias: Option<Ident>,
    pub lock_type: LockTableType,
}

impl fmt::Display for LockTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            table: tbl_name,
            alias,
            lock_type,
        } = self;

        write!(f, "{tbl_name} ")?;
        if let Some(alias) = alias {
            write!(f, "AS {alias} ")?;
        }
        write!(f, "{lock_type}")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum LockTableType {
    Read { local: bool },
    Write { low_priority: bool },
}

impl fmt::Display for LockTableType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { local } => {
                write!(f, "READ")?;
                if *local {
                    write!(f, " LOCAL")?;
                }
            }
            Self::Write { low_priority } => {
                if *low_priority {
                    write!(f, "LOW_PRIORITY ")?;
                }
                write!(f, "WRITE")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct HiveSetLocation {
    pub has_set: bool,
    pub location: Ident,
}

impl fmt::Display for HiveSetLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.has_set {
            write!(f, "SET ")?;
        }
        write!(f, "LOCATION {}", self.location)
    }
}

/// MySQL `ALTER TABLE` only  [FIRST | AFTER column_name]
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MySQLColumnPosition {
    First,
    After(Ident),
}

impl Display for MySQLColumnPosition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MySQLColumnPosition::First => write!(f, "FIRST"),
            MySQLColumnPosition::After(ident) => {
                let column_name = &ident.value;
                write!(f, "AFTER {column_name}")
            }
        }
    }
}

/// MySQL `CREATE VIEW` algorithm parameter: [ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CreateViewAlgorithm {
    Undefined,
    Merge,
    TempTable,
}

impl Display for CreateViewAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CreateViewAlgorithm::Undefined => write!(f, "UNDEFINED"),
            CreateViewAlgorithm::Merge => write!(f, "MERGE"),
            CreateViewAlgorithm::TempTable => write!(f, "TEMPTABLE"),
        }
    }
}
/// MySQL `CREATE VIEW` security parameter: [SQL SECURITY { DEFINER | INVOKER }]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CreateViewSecurity {
    Definer,
    Invoker,
}

impl Display for CreateViewSecurity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CreateViewSecurity::Definer => write!(f, "DEFINER"),
            CreateViewSecurity::Invoker => write!(f, "INVOKER"),
        }
    }
}

/// [MySQL] `CREATE VIEW` additional parameters
///
/// [MySQL]: https://dev.mysql.com/doc/refman/9.1/en/create-view.html
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateViewParams {
    pub algorithm: Option<CreateViewAlgorithm>,
    pub definer: Option<GranteeName>,
    pub security: Option<CreateViewSecurity>,
}

impl Display for CreateViewParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let CreateViewParams {
            algorithm,
            definer,
            security,
        } = self;
        if let Some(algorithm) = algorithm {
            write!(f, "ALGORITHM = {algorithm} ")?;
        }
        if let Some(definers) = definer {
            write!(f, "DEFINER = {definers} ")?;
        }
        if let Some(security) = security {
            write!(f, "SQL SECURITY {security} ")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// Key/Value, where the value is a (optionally named) list of identifiers
///
/// ```sql
/// UNION = (tbl_name[,tbl_name]...)
/// ENGINE = ReplicatedMergeTree('/table_name','{replica}', ver)
/// ENGINE = SummingMergeTree([columns])
/// ```
pub struct NamedParenthesizedList {
    pub key: Ident,
    pub name: Option<Ident>,
    pub values: Vec<Ident>,
}

/// Snowflake `WITH ROW ACCESS POLICY policy_name ON (identifier, ...)`
///
/// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
/// <https://docs.snowflake.com/en/user-guide/security-row-intro>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RowAccessPolicy {
    pub policy: ObjectName,
    pub on: Vec<Ident>,
}

impl RowAccessPolicy {
    pub fn new(policy: ObjectName, on: Vec<Ident>) -> Self {
        Self { policy, on }
    }
}

impl Display for RowAccessPolicy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "WITH ROW ACCESS POLICY {} ON ({})",
            self.policy,
            display_comma_separated(self.on.as_slice())
        )
    }
}

/// Snowflake `WITH TAG ( tag_name = '<tag_value>', ...)`
///
/// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Tag {
    pub key: ObjectName,
    pub value: String,
}

impl Tag {
    pub fn new(key: ObjectName, value: String) -> Self {
        Self { key, value }
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}='{}'", self.key, self.value)
    }
}

/// Snowflake `WITH CONTACT ( purpose = contact [ , purpose = contact ...] )`
///
/// <https://docs.snowflake.com/en/sql-reference/sql/create-database>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ContactEntry {
    pub purpose: String,
    pub contact: String,
}

impl Display for ContactEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.purpose, self.contact)
    }
}

/// Helper to indicate if a comment includes the `=` in the display form
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CommentDef {
    /// Includes `=` when printing the comment, as `COMMENT = 'comment'`
    /// Does not include `=` when printing the comment, as `COMMENT 'comment'`
    WithEq(String),
    WithoutEq(String),
}

impl Display for CommentDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommentDef::WithEq(comment) | CommentDef::WithoutEq(comment) => write!(f, "{comment}"),
        }
    }
}

/// Helper to indicate if a collection should be wrapped by a symbol in the display form
///
/// [`Display`] is implemented for every [`Vec<T>`] where `T: Display`.
/// The string output is a comma separated list for the vec items
///
/// # Examples
/// ```
/// # use sqlparser::ast::WrappedCollection;
/// let items = WrappedCollection::Parentheses(vec!["one", "two", "three"]);
/// assert_eq!("(one, two, three)", items.to_string());
///
/// let items = WrappedCollection::NoWrapping(vec!["one", "two", "three"]);
/// assert_eq!("one, two, three", items.to_string());
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum WrappedCollection<T> {
    /// Print the collection without wrapping symbols, as `item, item, item`
    NoWrapping(T),
    /// Wraps the collection in Parentheses, as `(item, item, item)`
    Parentheses(T),
}

impl<T> Display for WrappedCollection<Vec<T>>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WrappedCollection::NoWrapping(inner) => {
                write!(f, "{}", display_comma_separated(inner.as_slice()))
            }
            WrappedCollection::Parentheses(inner) => {
                write!(f, "({})", display_comma_separated(inner.as_slice()))
            }
        }
    }
}

/// Represents a single PostgreSQL utility option.
///
/// A utility option is a key-value pair where the key is an identifier (IDENT) and the value
/// can be one of the following:
/// - A number with an optional sign (`+` or `-`). Example: `+10`, `-10.2`, `3`
/// - A non-keyword string. Example: `option1`, `'option2'`, `"option3"`
/// - keyword: `TRUE`, `FALSE`, `ON` (`off` is also accept).
/// - Empty. Example: `ANALYZE` (identifier only)
///
/// Utility options are used in various PostgreSQL DDL statements, including statements such as
/// `CLUSTER`, `EXPLAIN`, `VACUUM`, and `REINDEX`. These statements format options as `( option [, ...] )`.
///
/// [CLUSTER](https://www.postgresql.org/docs/current/sql-cluster.html)
/// [EXPLAIN](https://www.postgresql.org/docs/current/sql-explain.html)
/// [VACUUM](https://www.postgresql.org/docs/current/sql-vacuum.html)
/// [REINDEX](https://www.postgresql.org/docs/current/sql-reindex.html)
///
/// For example, the `EXPLAIN` AND `VACUUM` statements with options might look like this:
/// ```sql
/// EXPLAIN (ANALYZE, VERBOSE TRUE, FORMAT TEXT) SELECT * FROM my_table;
///
/// VACUUM (VERBOSE, ANALYZE ON, PARALLEL 10) my_table;
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct UtilityOption {
    pub name: Ident,
    pub arg: Option<Expr>,
}

impl Display for UtilityOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref arg) = self.arg {
            write!(f, "{} {}", self.name, arg)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

/// Represents the different options available for `SHOW`
/// statements to filter the results. Example from Snowflake:
/// <https://docs.snowflake.com/en/sql-reference/sql/show-tables>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowStatementOptions {
    pub show_in: Option<ShowStatementIn>,
    pub starts_with: Option<Value>,
    pub limit: Option<Expr>,
    pub limit_from: Option<Value>,
    pub filter_position: Option<ShowStatementFilterPosition>,
}

impl Display for ShowStatementOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (like_in_infix, like_in_suffix) = match &self.filter_position {
            Some(ShowStatementFilterPosition::Infix(filter)) => {
                (format!(" {filter}"), "".to_string())
            }
            Some(ShowStatementFilterPosition::Suffix(filter)) => {
                ("".to_string(), format!(" {filter}"))
            }
            None => ("".to_string(), "".to_string()),
        };
        write!(
            f,
            "{like_in_infix}{show_in}{starts_with}{limit}{from}{like_in_suffix}",
            show_in = match &self.show_in {
                Some(i) => format!(" {i}"),
                None => String::new(),
            },
            starts_with = match &self.starts_with {
                Some(s) => format!(" STARTS WITH {s}"),
                None => String::new(),
            },
            limit = match &self.limit {
                Some(l) => format!(" LIMIT {l}"),
                None => String::new(),
            },
            from = match &self.limit_from {
                Some(f) => format!(" FROM {f}"),
                None => String::new(),
            }
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ShowStatementFilterPosition {
    Infix(ShowStatementFilter), // For example: SHOW COLUMNS LIKE '%name%' IN TABLE tbl
    Suffix(ShowStatementFilter), // For example: SHOW COLUMNS IN tbl LIKE '%name%'
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ShowStatementInParentType {
    Account,
    Database,
    Schema,
    Table,
    View,
}

impl fmt::Display for ShowStatementInParentType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ShowStatementInParentType::Account => write!(f, "ACCOUNT"),
            ShowStatementInParentType::Database => write!(f, "DATABASE"),
            ShowStatementInParentType::Schema => write!(f, "SCHEMA"),
            ShowStatementInParentType::Table => write!(f, "TABLE"),
            ShowStatementInParentType::View => write!(f, "VIEW"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowStatementIn {
    pub clause: ShowStatementInClause,
    pub parent_type: Option<ShowStatementInParentType>,
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub parent_name: Option<ObjectName>,
}

impl fmt::Display for ShowStatementIn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.clause)?;
        if let Some(parent_type) = &self.parent_type {
            write!(f, " {parent_type}")?;
        }
        if let Some(parent_name) = &self.parent_name {
            write!(f, " {parent_name}")?;
        }
        Ok(())
    }
}

/// A Show Charset statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowCharset {
    /// The statement can be written as `SHOW CHARSET` or `SHOW CHARACTER SET`
    /// true means CHARSET was used and false means CHARACTER SET was used
    pub is_shorthand: bool,
    pub filter: Option<ShowStatementFilter>,
}

impl fmt::Display for ShowCharset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SHOW")?;
        if self.is_shorthand {
            write!(f, " CHARSET")?;
        } else {
            write!(f, " CHARACTER SET")?;
        }
        if let Some(filter) = &self.filter {
            write!(f, " {filter}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ShowObjects {
    pub terse: bool,
    pub show_options: ShowStatementOptions,
}

/// MSSQL's json null clause
///
/// ```plaintext
/// <json_null_clause> ::=
///       NULL ON NULL
///     | ABSENT ON NULL
/// ```
///
/// <https://learn.microsoft.com/en-us/sql/t-sql/functions/json-object-transact-sql?view=sql-server-ver16#json_null_clause>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JsonNullClause {
    NullOnNull,
    AbsentOnNull,
}

impl Display for JsonNullClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonNullClause::NullOnNull => write!(f, "NULL ON NULL"),
            JsonNullClause::AbsentOnNull => write!(f, "ABSENT ON NULL"),
        }
    }
}

/// PostgreSQL JSON function RETURNING clause
///
/// Example:
/// ```sql
/// JSON_OBJECT('a': 1 RETURNING jsonb)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct JsonReturningClause {
    pub data_type: DataType,
}

impl Display for JsonReturningClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RETURNING {}", self.data_type)
    }
}

/// rename object definition
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct RenameTable {
    pub old_name: ObjectName,
    pub new_name: ObjectName,
}

impl fmt::Display for RenameTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} TO {}", self.old_name, self.new_name)?;
        Ok(())
    }
}

/// Represents the referenced table in an `INSERT INTO` statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableObject {
    /// Table specified by name.
    /// Example:
    /// ```sql
    /// INSERT INTO my_table
    /// ```
    TableName(#[cfg_attr(feature = "visitor", visit(with = "visit_relation"))] ObjectName),

    /// Table specified as a function.
    /// Example:
    /// ```sql
    /// INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    /// ```
    /// [Clickhouse](https://clickhouse.com/docs/en/sql-reference/table-functions)
    TableFunction(Function),
}

impl fmt::Display for TableObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::TableName(table_name) => write!(f, "{table_name}"),
            Self::TableFunction(func) => write!(f, "FUNCTION {func}"),
        }
    }
}

/// Represents a SET SESSION AUTHORIZATION statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SetSessionAuthorizationParam {
    pub scope: ContextModifier,
    pub kind: SetSessionAuthorizationParamKind,
}

impl fmt::Display for SetSessionAuthorizationParam {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.kind)
    }
}

/// Represents the parameter kind for SET SESSION AUTHORIZATION
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetSessionAuthorizationParamKind {
    /// Default authorization
    Default,

    /// User name
    User(Ident),
}

impl fmt::Display for SetSessionAuthorizationParamKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetSessionAuthorizationParamKind::Default => write!(f, "DEFAULT"),
            SetSessionAuthorizationParamKind::User(name) => write!(f, "{}", name),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetSessionParamKind {
    Generic(SetSessionParamGeneric),
    IdentityInsert(SetSessionParamIdentityInsert),
    Offsets(SetSessionParamOffsets),
    Statistics(SetSessionParamStatistics),
}

impl fmt::Display for SetSessionParamKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetSessionParamKind::Generic(x) => write!(f, "{x}"),
            SetSessionParamKind::IdentityInsert(x) => write!(f, "{x}"),
            SetSessionParamKind::Offsets(x) => write!(f, "{x}"),
            SetSessionParamKind::Statistics(x) => write!(f, "{x}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SetSessionParamGeneric {
    pub names: Vec<String>,
    pub value: String,
}

impl fmt::Display for SetSessionParamGeneric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", display_comma_separated(&self.names), self.value)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SetSessionParamIdentityInsert {
    pub obj: ObjectName,
    pub value: SessionParamValue,
}

impl fmt::Display for SetSessionParamIdentityInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IDENTITY_INSERT {} {}", self.obj, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SetSessionParamOffsets {
    pub keywords: Vec<String>,
    pub value: SessionParamValue,
}

impl fmt::Display for SetSessionParamOffsets {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "OFFSETS {} {}",
            display_comma_separated(&self.keywords),
            self.value
        )
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SetSessionParamStatistics {
    pub topic: SessionParamStatsTopic,
    pub value: SessionParamValue,
}

impl fmt::Display for SetSessionParamStatistics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "STATISTICS {} {}", self.topic, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SessionParamStatsTopic {
    IO,
    Profile,
    Time,
    Xml,
}

impl fmt::Display for SessionParamStatsTopic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SessionParamStatsTopic::IO => write!(f, "IO"),
            SessionParamStatsTopic::Profile => write!(f, "PROFILE"),
            SessionParamStatsTopic::Time => write!(f, "TIME"),
            SessionParamStatsTopic::Xml => write!(f, "XML"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SessionParamValue {
    On,
    Off,
}

impl fmt::Display for SessionParamValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SessionParamValue::On => write!(f, "ON"),
            SessionParamValue::Off => write!(f, "OFF"),
        }
    }
}

/// Snowflake StorageSerializationPolicy for Iceberg Tables
/// ```sql
/// [ STORAGE_SERIALIZATION_POLICY = { COMPATIBLE | OPTIMIZED } ]
/// ```
///
/// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum StorageSerializationPolicy {
    Compatible,
    Optimized,
}

impl Display for StorageSerializationPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageSerializationPolicy::Compatible => write!(f, "COMPATIBLE"),
            StorageSerializationPolicy::Optimized => write!(f, "OPTIMIZED"),
        }
    }
}

/// Snowflake CatalogSyncNamespaceMode
/// ```sql
/// [ CATALOG_SYNC_NAMESPACE_MODE = { NEST | FLATTEN } ]
/// ```
///
/// <https://docs.snowflake.com/en/sql-reference/sql/create-database>
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CatalogSyncNamespaceMode {
    Nest,
    Flatten,
}

impl Display for CatalogSyncNamespaceMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogSyncNamespaceMode::Nest => write!(f, "NEST"),
            CatalogSyncNamespaceMode::Flatten => write!(f, "FLATTEN"),
        }
    }
}

/// Variants of the Snowflake `COPY INTO` statement
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CopyIntoSnowflakeKind {
    /// Loads data from files to a table
    /// See: <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table>
    Table,
    /// Unloads data from a table or query to external files
    /// See: <https://docs.snowflake.com/en/sql-reference/sql/copy-into-location>
    Location,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct PrintStatement {
    pub message: Box<Expr>,
}

impl fmt::Display for PrintStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PRINT {}", self.message)
    }
}

/// Represents a `Return` statement.
///
/// [MsSql triggers](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-trigger-transact-sql)
/// [MsSql functions](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-function-transact-sql)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ReturnStatement {
    pub value: Option<ReturnStatementValue>,
}

impl fmt::Display for ReturnStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.value {
            Some(ReturnStatementValue::Expr(expr)) => write!(f, "RETURN {expr}"),
            None => write!(f, "RETURN"),
        }
    }
}

/// Variants of a `RETURN` statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ReturnStatementValue {
    Expr(Expr),
}

/// Represents an `OPEN` statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OpenStatement {
    /// Cursor name
    pub cursor_name: Ident,
}

impl fmt::Display for OpenStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OPEN {}", self.cursor_name)
    }
}

/// Specifies Include / Exclude NULL within UNPIVOT command.
/// For example
/// `UNPIVOT (column1 FOR new_column IN (col3, col4, col5, col6))`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum NullInclusion {
    IncludeNulls,
    ExcludeNulls,
}

impl fmt::Display for NullInclusion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NullInclusion::IncludeNulls => write!(f, "INCLUDE NULLS"),
            NullInclusion::ExcludeNulls => write!(f, "EXCLUDE NULLS"),
        }
    }
}

/// Checks membership of a value in a JSON array
///
/// Syntax:
/// ```sql
/// <value> MEMBER OF(<array>)
/// ```
/// [MySQL](https://dev.mysql.com/doc/refman/8.4/en/json-search-functions.html#operator_member-of)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct MemberOf {
    pub value: Box<Expr>,
    pub array: Box<Expr>,
}

impl fmt::Display for MemberOf {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} MEMBER OF({})", self.value, self.array)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ExportData {
    pub options: Vec<SqlOption>,
    pub query: Box<Query>,
    pub connection: Option<ObjectName>,
}

impl fmt::Display for ExportData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(connection) = &self.connection {
            write!(
                f,
                "EXPORT DATA WITH CONNECTION {connection} OPTIONS({}) AS {}",
                display_comma_separated(&self.options),
                self.query
            )
        } else {
            write!(
                f,
                "EXPORT DATA OPTIONS({}) AS {}",
                display_comma_separated(&self.options),
                self.query
            )
        }
    }
}
/// Creates a user
///
/// Syntax:
/// ```sql
/// CREATE [OR REPLACE] USER [IF NOT EXISTS] <name> [OPTIONS]
/// ```
///
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-user)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateUser {
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub name: Ident,
    pub options: KeyValueOptions,
    pub with_tags: bool,
    pub tags: KeyValueOptions,
}

impl fmt::Display for CreateUser {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE")?;
        if self.or_replace {
            write!(f, " OR REPLACE")?;
        }
        write!(f, " USER")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.name)?;
        if !self.options.options.is_empty() {
            write!(f, " {}", self.options)?;
        }
        if !self.tags.options.is_empty() {
            if self.with_tags {
                write!(f, " WITH")?;
            }
            write!(f, " TAG ({})", self.tags)?;
        }
        Ok(())
    }
}

/// Modifies the properties of a user
///
/// Syntax:
/// ```sql
/// ALTER USER [ IF EXISTS ] [ <name> ] [ OPTIONS ]
/// ```
///
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/alter-user)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterUser {
    pub if_exists: bool,
    pub name: Ident,
    /// The following fields are Snowflake-specific: <https://docs.snowflake.com/en/sql-reference/sql/alter-user#syntax>
    pub rename_to: Option<Ident>,
    pub reset_password: bool,
    pub abort_all_queries: bool,
    pub add_role_delegation: Option<AlterUserAddRoleDelegation>,
    pub remove_role_delegation: Option<AlterUserRemoveRoleDelegation>,
    pub enroll_mfa: bool,
    pub set_default_mfa_method: Option<MfaMethodKind>,
    pub remove_mfa_method: Option<MfaMethodKind>,
    pub modify_mfa_method: Option<AlterUserModifyMfaMethod>,
    pub add_mfa_method_otp: Option<AlterUserAddMfaMethodOtp>,
    pub set_policy: Option<AlterUserSetPolicy>,
    pub unset_policy: Option<UserPolicyKind>,
    pub set_tag: KeyValueOptions,
    pub unset_tag: Vec<String>,
    pub set_props: KeyValueOptions,
    pub unset_props: Vec<String>,
}

/// ```sql
/// ALTER USER [ IF EXISTS ] [ <name> ] ADD DELEGATED AUTHORIZATION OF ROLE <role_name> TO SECURITY INTEGRATION <integration_name>
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterUserAddRoleDelegation {
    pub role: Ident,
    pub integration: Ident,
}

/// ```sql
/// ALTER USER [ IF EXISTS ] [ <name> ] REMOVE DELEGATED { AUTHORIZATION OF ROLE <role_name> | AUTHORIZATIONS } FROM SECURITY INTEGRATION <integration_name>
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterUserRemoveRoleDelegation {
    pub role: Option<Ident>,
    pub integration: Ident,
}

/// ```sql
/// ADD MFA METHOD OTP [ COUNT = number ]
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterUserAddMfaMethodOtp {
    pub count: Option<Value>,
}

/// ```sql
/// ALTER USER [ IF EXISTS ] [ <name> ] MODIFY MFA METHOD <mfa_method> SET COMMENT = '<string>'
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterUserModifyMfaMethod {
    pub method: MfaMethodKind,
    pub comment: String,
}

/// Types of MFA methods
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MfaMethodKind {
    PassKey,
    Totp,
    Duo,
}

impl fmt::Display for MfaMethodKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MfaMethodKind::PassKey => write!(f, "PASSKEY"),
            MfaMethodKind::Totp => write!(f, "TOTP"),
            MfaMethodKind::Duo => write!(f, "DUO"),
        }
    }
}

/// ```sql
/// ALTER USER [ IF EXISTS ] [ <name> ] SET { AUTHENTICATION | PASSWORD | SESSION } POLICY <policy_name>
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AlterUserSetPolicy {
    pub policy_kind: UserPolicyKind,
    pub policy: Ident,
}

/// Types of user-based policies
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum UserPolicyKind {
    Authentication,
    Password,
    Session,
}

impl fmt::Display for UserPolicyKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserPolicyKind::Authentication => write!(f, "AUTHENTICATION"),
            UserPolicyKind::Password => write!(f, "PASSWORD"),
            UserPolicyKind::Session => write!(f, "SESSION"),
        }
    }
}

impl fmt::Display for AlterUser {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ALTER")?;
        write!(f, " USER")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.name)?;
        if let Some(new_name) = &self.rename_to {
            write!(f, " RENAME TO {new_name}")?;
        }
        if self.reset_password {
            write!(f, " RESET PASSWORD")?;
        }
        if self.abort_all_queries {
            write!(f, " ABORT ALL QUERIES")?;
        }
        if let Some(role_delegation) = &self.add_role_delegation {
            let role = &role_delegation.role;
            let integration = &role_delegation.integration;
            write!(
                f,
                " ADD DELEGATED AUTHORIZATION OF ROLE {role} TO SECURITY INTEGRATION {integration}"
            )?;
        }
        if let Some(role_delegation) = &self.remove_role_delegation {
            write!(f, " REMOVE DELEGATED")?;
            match &role_delegation.role {
                Some(role) => write!(f, " AUTHORIZATION OF ROLE {role}")?,
                None => write!(f, " AUTHORIZATIONS")?,
            }
            let integration = &role_delegation.integration;
            write!(f, " FROM SECURITY INTEGRATION {integration}")?;
        }
        if self.enroll_mfa {
            write!(f, " ENROLL MFA")?;
        }
        if let Some(method) = &self.set_default_mfa_method {
            write!(f, " SET DEFAULT_MFA_METHOD {method}")?
        }
        if let Some(method) = &self.remove_mfa_method {
            write!(f, " REMOVE MFA METHOD {method}")?;
        }
        if let Some(modify) = &self.modify_mfa_method {
            let method = &modify.method;
            let comment = &modify.comment;
            write!(
                f,
                " MODIFY MFA METHOD {method} SET COMMENT '{}'",
                value::escape_single_quote_string(comment)
            )?;
        }
        if let Some(add_mfa_method_otp) = &self.add_mfa_method_otp {
            write!(f, " ADD MFA METHOD OTP")?;
            if let Some(count) = &add_mfa_method_otp.count {
                write!(f, " COUNT = {count}")?;
            }
        }
        if let Some(policy) = &self.set_policy {
            let policy_kind = &policy.policy_kind;
            let name = &policy.policy;
            write!(f, " SET {policy_kind} POLICY {name}")?;
        }
        if let Some(policy_kind) = &self.unset_policy {
            write!(f, " UNSET {policy_kind} POLICY")?;
        }
        if !self.set_tag.options.is_empty() {
            write!(f, " SET TAG {}", self.set_tag)?;
        }
        if !self.unset_tag.is_empty() {
            write!(f, " UNSET TAG {}", display_comma_separated(&self.unset_tag))?;
        }
        let has_props = !self.set_props.options.is_empty();
        if has_props {
            write!(f, " SET")?;
            write!(f, " {}", &self.set_props)?;
        }
        if !self.unset_props.is_empty() {
            write!(f, " UNSET {}", display_comma_separated(&self.unset_props))?;
        }
        Ok(())
    }
}

/// Specifies how to create a new table based on an existing table's schema.
/// '''sql
/// CREATE TABLE new LIKE old ...
/// '''
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CreateTableLikeKind {
    /// '''sql
    /// CREATE TABLE new (LIKE old ...)
    /// '''
    /// [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)
    Parenthesized(CreateTableLike),
    /// '''sql
    /// CREATE TABLE new LIKE old ...
    /// '''
    /// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-table#label-create-table-like)
    /// [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_table_like)
    Plain(CreateTableLike),
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CreateTableLikeDefaults {
    Including,
    Excluding,
}

impl fmt::Display for CreateTableLikeDefaults {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CreateTableLikeDefaults::Including => write!(f, "INCLUDING DEFAULTS"),
            CreateTableLikeDefaults::Excluding => write!(f, "EXCLUDING DEFAULTS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateTableLike {
    pub name: ObjectName,
    pub defaults: Option<CreateTableLikeDefaults>,
}

impl fmt::Display for CreateTableLike {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LIKE {}", self.name)?;
        if let Some(defaults) = &self.defaults {
            write!(f, " {defaults}")?;
        }
        Ok(())
    }
}

/// Specifies the refresh mode for the dynamic table.
///
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table)
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RefreshModeKind {
    Auto,
    Full,
    Incremental,
}

impl fmt::Display for RefreshModeKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RefreshModeKind::Auto => write!(f, "AUTO"),
            RefreshModeKind::Full => write!(f, "FULL"),
            RefreshModeKind::Incremental => write!(f, "INCREMENTAL"),
        }
    }
}

/// Specifies the behavior of the initial refresh of the dynamic table.
///
/// [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table)
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum InitializeKind {
    OnCreate,
    OnSchedule,
}

impl fmt::Display for InitializeKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InitializeKind::OnCreate => write!(f, "ON_CREATE"),
            InitializeKind::OnSchedule => write!(f, "ON_SCHEDULE"),
        }
    }
}

/// Re-sorts rows and reclaims space in either a specified table or all tables in the current database
///
/// '''sql
/// VACUUM [ FULL | SORT ONLY | DELETE ONLY | REINDEX | RECLUSTER ] [ \[ table_name \] [ TO threshold PERCENT ] \[ BOOST \] ]
/// '''
/// [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/r_VACUUM_command.html)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct VacuumStatement {
    pub full: bool,
    pub sort_only: bool,
    pub delete_only: bool,
    pub reindex: bool,
    pub recluster: bool,
    pub table_name: Option<ObjectName>,
    pub threshold: Option<Value>,
    pub boost: bool,
}

impl fmt::Display for VacuumStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "VACUUM{}{}{}{}{}",
            if self.full { " FULL" } else { "" },
            if self.sort_only { " SORT ONLY" } else { "" },
            if self.delete_only { " DELETE ONLY" } else { "" },
            if self.reindex { " REINDEX" } else { "" },
            if self.recluster { " RECLUSTER" } else { "" },
        )?;
        if let Some(table_name) = &self.table_name {
            write!(f, " {table_name}")?;
        }
        if let Some(threshold) = &self.threshold {
            write!(f, " TO {threshold} PERCENT")?;
        }
        if self.boost {
            write!(f, " BOOST")?;
        }
        Ok(())
    }
}

/// Variants of the RESET statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Reset {
    /// Resets all session parameters to their default values.
    ALL,

    /// Resets a specific session parameter to its default value.
    ConfigurationParameter(ObjectName),
}

/// Resets a session parameter to its default value.
/// ```sql
/// RESET { ALL | <configuration_parameter> }
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ResetStatement {
    pub reset: Reset,
}

impl fmt::Display for ResetStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.reset {
            Reset::ALL => write!(f, "RESET ALL"),
            Reset::ConfigurationParameter(param) => write!(f, "RESET {}", param),
        }
    }
}

impl From<Set> for Statement {
    fn from(s: Set) -> Self {
        Self::Set(s)
    }
}

impl From<Query> for Statement {
    fn from(q: Query) -> Self {
        Box::new(q).into()
    }
}

impl From<Box<Query>> for Statement {
    fn from(q: Box<Query>) -> Self {
        Self::Query(q)
    }
}

impl From<Insert> for Statement {
    fn from(i: Insert) -> Self {
        Self::Insert(i)
    }
}

impl From<Update> for Statement {
    fn from(u: Update) -> Self {
        Self::Update(u)
    }
}

impl From<CreateView> for Statement {
    fn from(cv: CreateView) -> Self {
        Self::CreateView(cv)
    }
}

impl From<CreateRole> for Statement {
    fn from(cr: CreateRole) -> Self {
        Self::CreateRole(cr)
    }
}

impl From<AlterTable> for Statement {
    fn from(at: AlterTable) -> Self {
        Self::AlterTable(at)
    }
}

impl From<DropFunction> for Statement {
    fn from(df: DropFunction) -> Self {
        Self::DropFunction(df)
    }
}

impl From<CreateExtension> for Statement {
    fn from(ce: CreateExtension) -> Self {
        Self::CreateExtension(ce)
    }
}

impl From<DropExtension> for Statement {
    fn from(de: DropExtension) -> Self {
        Self::DropExtension(de)
    }
}

impl From<CaseStatement> for Statement {
    fn from(c: CaseStatement) -> Self {
        Self::Case(c)
    }
}

impl From<IfStatement> for Statement {
    fn from(i: IfStatement) -> Self {
        Self::If(i)
    }
}

impl From<WhileStatement> for Statement {
    fn from(w: WhileStatement) -> Self {
        Self::While(w)
    }
}

impl From<RaiseStatement> for Statement {
    fn from(r: RaiseStatement) -> Self {
        Self::Raise(r)
    }
}

impl From<Function> for Statement {
    fn from(f: Function) -> Self {
        Self::Call(f)
    }
}

impl From<OpenStatement> for Statement {
    fn from(o: OpenStatement) -> Self {
        Self::Open(o)
    }
}

impl From<Delete> for Statement {
    fn from(d: Delete) -> Self {
        Self::Delete(d)
    }
}

impl From<CreateTable> for Statement {
    fn from(c: CreateTable) -> Self {
        Self::CreateTable(c)
    }
}

impl From<CreateIndex> for Statement {
    fn from(c: CreateIndex) -> Self {
        Self::CreateIndex(c)
    }
}

impl From<CreateServerStatement> for Statement {
    fn from(c: CreateServerStatement) -> Self {
        Self::CreateServer(c)
    }
}

impl From<CreateConnector> for Statement {
    fn from(c: CreateConnector) -> Self {
        Self::CreateConnector(c)
    }
}

impl From<AlterSchema> for Statement {
    fn from(a: AlterSchema) -> Self {
        Self::AlterSchema(a)
    }
}

impl From<AlterType> for Statement {
    fn from(a: AlterType) -> Self {
        Self::AlterType(a)
    }
}

impl From<DropDomain> for Statement {
    fn from(d: DropDomain) -> Self {
        Self::DropDomain(d)
    }
}

impl From<ShowCharset> for Statement {
    fn from(s: ShowCharset) -> Self {
        Self::ShowCharset(s)
    }
}

impl From<ShowObjects> for Statement {
    fn from(s: ShowObjects) -> Self {
        Self::ShowObjects(s)
    }
}

impl From<Use> for Statement {
    fn from(u: Use) -> Self {
        Self::Use(u)
    }
}

impl From<CreateFunction> for Statement {
    fn from(c: CreateFunction) -> Self {
        Self::CreateFunction(c)
    }
}

impl From<CreateTrigger> for Statement {
    fn from(c: CreateTrigger) -> Self {
        Self::CreateTrigger(c)
    }
}

impl From<DropTrigger> for Statement {
    fn from(d: DropTrigger) -> Self {
        Self::DropTrigger(d)
    }
}

impl From<DenyStatement> for Statement {
    fn from(d: DenyStatement) -> Self {
        Self::Deny(d)
    }
}

impl From<CreateDomain> for Statement {
    fn from(c: CreateDomain) -> Self {
        Self::CreateDomain(c)
    }
}

impl From<RenameTable> for Statement {
    fn from(r: RenameTable) -> Self {
        vec![r].into()
    }
}

impl From<Vec<RenameTable>> for Statement {
    fn from(r: Vec<RenameTable>) -> Self {
        Self::RenameTable(r)
    }
}

impl From<PrintStatement> for Statement {
    fn from(p: PrintStatement) -> Self {
        Self::Print(p)
    }
}

impl From<ReturnStatement> for Statement {
    fn from(r: ReturnStatement) -> Self {
        Self::Return(r)
    }
}

impl From<ExportData> for Statement {
    fn from(e: ExportData) -> Self {
        Self::ExportData(e)
    }
}

impl From<CreateUser> for Statement {
    fn from(c: CreateUser) -> Self {
        Self::CreateUser(c)
    }
}

impl From<VacuumStatement> for Statement {
    fn from(v: VacuumStatement) -> Self {
        Self::Vacuum(v)
    }
}

impl From<ResetStatement> for Statement {
    fn from(r: ResetStatement) -> Self {
        Self::Reset(r)
    }
}

#[cfg(test)]
mod tests {
    use crate::tokenizer::Location;

    use super::*;

    #[test]
    fn test_window_frame_default() {
        let window_frame = WindowFrame::default();
        assert_eq!(WindowFrameBound::Preceding(None), window_frame.start_bound);
    }

    #[test]
    fn test_grouping_sets_display() {
        // a and b in different group
        let grouping_sets = Expr::GroupingSets(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![Expr::Identifier(Ident::new("b"))],
        ]);
        assert_eq!("GROUPING SETS ((a), (b))", format!("{grouping_sets}"));

        // a and b in the same group
        let grouping_sets = Expr::GroupingSets(vec![vec![
            Expr::Identifier(Ident::new("a")),
            Expr::Identifier(Ident::new("b")),
        ]]);
        assert_eq!("GROUPING SETS ((a, b))", format!("{grouping_sets}"));

        // (a, b) and (c, d) in different group
        let grouping_sets = Expr::GroupingSets(vec![
            vec![
                Expr::Identifier(Ident::new("a")),
                Expr::Identifier(Ident::new("b")),
            ],
            vec![
                Expr::Identifier(Ident::new("c")),
                Expr::Identifier(Ident::new("d")),
            ],
        ]);
        assert_eq!("GROUPING SETS ((a, b), (c, d))", format!("{grouping_sets}"));
    }

    #[test]
    fn test_rollup_display() {
        let rollup = Expr::Rollup(vec![vec![Expr::Identifier(Ident::new("a"))]]);
        assert_eq!("ROLLUP (a)", format!("{rollup}"));

        let rollup = Expr::Rollup(vec![vec![
            Expr::Identifier(Ident::new("a")),
            Expr::Identifier(Ident::new("b")),
        ]]);
        assert_eq!("ROLLUP ((a, b))", format!("{rollup}"));

        let rollup = Expr::Rollup(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![Expr::Identifier(Ident::new("b"))],
        ]);
        assert_eq!("ROLLUP (a, b)", format!("{rollup}"));

        let rollup = Expr::Rollup(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![
                Expr::Identifier(Ident::new("b")),
                Expr::Identifier(Ident::new("c")),
            ],
            vec![Expr::Identifier(Ident::new("d"))],
        ]);
        assert_eq!("ROLLUP (a, (b, c), d)", format!("{rollup}"));
    }

    #[test]
    fn test_cube_display() {
        let cube = Expr::Cube(vec![vec![Expr::Identifier(Ident::new("a"))]]);
        assert_eq!("CUBE (a)", format!("{cube}"));

        let cube = Expr::Cube(vec![vec![
            Expr::Identifier(Ident::new("a")),
            Expr::Identifier(Ident::new("b")),
        ]]);
        assert_eq!("CUBE ((a, b))", format!("{cube}"));

        let cube = Expr::Cube(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![Expr::Identifier(Ident::new("b"))],
        ]);
        assert_eq!("CUBE (a, b)", format!("{cube}"));

        let cube = Expr::Cube(vec![
            vec![Expr::Identifier(Ident::new("a"))],
            vec![
                Expr::Identifier(Ident::new("b")),
                Expr::Identifier(Ident::new("c")),
            ],
            vec![Expr::Identifier(Ident::new("d"))],
        ]);
        assert_eq!("CUBE (a, (b, c), d)", format!("{cube}"));
    }

    #[test]
    fn test_interval_display() {
        let interval = Expr::Interval(Interval {
            value: Box::new(Expr::Value(
                Value::SingleQuotedString(String::from("123:45.67")).with_empty_span(),
            )),
            leading_field: Some(DateTimeField::Minute),
            leading_precision: Some(10),
            last_field: Some(DateTimeField::Second),
            fractional_seconds_precision: Some(9),
        });
        assert_eq!(
            "INTERVAL '123:45.67' MINUTE (10) TO SECOND (9)",
            format!("{interval}"),
        );

        let interval = Expr::Interval(Interval {
            value: Box::new(Expr::Value(
                Value::SingleQuotedString(String::from("5")).with_empty_span(),
            )),
            leading_field: Some(DateTimeField::Second),
            leading_precision: Some(1),
            last_field: None,
            fractional_seconds_precision: Some(3),
        });
        assert_eq!("INTERVAL '5' SECOND (1, 3)", format!("{interval}"));
    }

    #[test]
    fn test_one_or_many_with_parens_deref() {
        use core::ops::Index;

        let one = OneOrManyWithParens::One("a");

        assert_eq!(one.deref(), &["a"]);
        assert_eq!(<OneOrManyWithParens<_> as Deref>::deref(&one), &["a"]);

        assert_eq!(one[0], "a");
        assert_eq!(one.index(0), &"a");
        assert_eq!(
            <<OneOrManyWithParens<_> as Deref>::Target as Index<usize>>::index(&one, 0),
            &"a"
        );

        assert_eq!(one.len(), 1);
        assert_eq!(<OneOrManyWithParens<_> as Deref>::Target::len(&one), 1);

        let many1 = OneOrManyWithParens::Many(vec!["b"]);

        assert_eq!(many1.deref(), &["b"]);
        assert_eq!(<OneOrManyWithParens<_> as Deref>::deref(&many1), &["b"]);

        assert_eq!(many1[0], "b");
        assert_eq!(many1.index(0), &"b");
        assert_eq!(
            <<OneOrManyWithParens<_> as Deref>::Target as Index<usize>>::index(&many1, 0),
            &"b"
        );

        assert_eq!(many1.len(), 1);
        assert_eq!(<OneOrManyWithParens<_> as Deref>::Target::len(&many1), 1);

        let many2 = OneOrManyWithParens::Many(vec!["c", "d"]);

        assert_eq!(many2.deref(), &["c", "d"]);
        assert_eq!(
            <OneOrManyWithParens<_> as Deref>::deref(&many2),
            &["c", "d"]
        );

        assert_eq!(many2[0], "c");
        assert_eq!(many2.index(0), &"c");
        assert_eq!(
            <<OneOrManyWithParens<_> as Deref>::Target as Index<usize>>::index(&many2, 0),
            &"c"
        );

        assert_eq!(many2[1], "d");
        assert_eq!(many2.index(1), &"d");
        assert_eq!(
            <<OneOrManyWithParens<_> as Deref>::Target as Index<usize>>::index(&many2, 1),
            &"d"
        );

        assert_eq!(many2.len(), 2);
        assert_eq!(<OneOrManyWithParens<_> as Deref>::Target::len(&many2), 2);
    }

    #[test]
    fn test_one_or_many_with_parens_as_ref() {
        let one = OneOrManyWithParens::One("a");

        assert_eq!(one.as_ref(), &["a"]);
        assert_eq!(<OneOrManyWithParens<_> as AsRef<_>>::as_ref(&one), &["a"]);

        let many1 = OneOrManyWithParens::Many(vec!["b"]);

        assert_eq!(many1.as_ref(), &["b"]);
        assert_eq!(<OneOrManyWithParens<_> as AsRef<_>>::as_ref(&many1), &["b"]);

        let many2 = OneOrManyWithParens::Many(vec!["c", "d"]);

        assert_eq!(many2.as_ref(), &["c", "d"]);
        assert_eq!(
            <OneOrManyWithParens<_> as AsRef<_>>::as_ref(&many2),
            &["c", "d"]
        );
    }

    #[test]
    fn test_one_or_many_with_parens_ref_into_iter() {
        let one = OneOrManyWithParens::One("a");

        assert_eq!(Vec::from_iter(&one), vec![&"a"]);

        let many1 = OneOrManyWithParens::Many(vec!["b"]);

        assert_eq!(Vec::from_iter(&many1), vec![&"b"]);

        let many2 = OneOrManyWithParens::Many(vec!["c", "d"]);

        assert_eq!(Vec::from_iter(&many2), vec![&"c", &"d"]);
    }

    #[test]
    fn test_one_or_many_with_parens_value_into_iter() {
        use core::iter::once;

        //tests that our iterator implemented methods behaves exactly as it's inner iterator, at every step up to n calls to next/next_back
        fn test_steps<I>(ours: OneOrManyWithParens<usize>, inner: I, n: usize)
        where
            I: IntoIterator<Item = usize, IntoIter: DoubleEndedIterator + Clone> + Clone,
        {
            fn checks<I>(ours: OneOrManyWithParensIntoIter<usize>, inner: I)
            where
                I: Iterator<Item = usize> + Clone + DoubleEndedIterator,
            {
                assert_eq!(ours.size_hint(), inner.size_hint());
                assert_eq!(ours.clone().count(), inner.clone().count());

                assert_eq!(
                    ours.clone().fold(1, |a, v| a + v),
                    inner.clone().fold(1, |a, v| a + v)
                );

                assert_eq!(Vec::from_iter(ours.clone()), Vec::from_iter(inner.clone()));
                assert_eq!(
                    Vec::from_iter(ours.clone().rev()),
                    Vec::from_iter(inner.clone().rev())
                );
            }

            let mut ours_next = ours.clone().into_iter();
            let mut inner_next = inner.clone().into_iter();

            for _ in 0..n {
                checks(ours_next.clone(), inner_next.clone());

                assert_eq!(ours_next.next(), inner_next.next());
            }

            let mut ours_next_back = ours.clone().into_iter();
            let mut inner_next_back = inner.clone().into_iter();

            for _ in 0..n {
                checks(ours_next_back.clone(), inner_next_back.clone());

                assert_eq!(ours_next_back.next_back(), inner_next_back.next_back());
            }

            let mut ours_mixed = ours.clone().into_iter();
            let mut inner_mixed = inner.clone().into_iter();

            for i in 0..n {
                checks(ours_mixed.clone(), inner_mixed.clone());

                if i % 2 == 0 {
                    assert_eq!(ours_mixed.next_back(), inner_mixed.next_back());
                } else {
                    assert_eq!(ours_mixed.next(), inner_mixed.next());
                }
            }

            let mut ours_mixed2 = ours.into_iter();
            let mut inner_mixed2 = inner.into_iter();

            for i in 0..n {
                checks(ours_mixed2.clone(), inner_mixed2.clone());

                if i % 2 == 0 {
                    assert_eq!(ours_mixed2.next(), inner_mixed2.next());
                } else {
                    assert_eq!(ours_mixed2.next_back(), inner_mixed2.next_back());
                }
            }
        }

        test_steps(OneOrManyWithParens::One(1), once(1), 3);
        test_steps(OneOrManyWithParens::Many(vec![2]), vec![2], 3);
        test_steps(OneOrManyWithParens::Many(vec![3, 4]), vec![3, 4], 4);
    }

    // Tests that the position in the code of an `Ident` does not affect its
    // ordering.
    #[test]
    fn test_ident_ord() {
        let mut a = Ident::with_span(Span::new(Location::new(1, 1), Location::new(1, 1)), "a");
        let mut b = Ident::with_span(Span::new(Location::new(2, 2), Location::new(2, 2)), "b");

        assert!(a < b);
        std::mem::swap(&mut a.span, &mut b.span);
        assert!(a < b);
    }
}
