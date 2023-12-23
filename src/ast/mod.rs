// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL Abstract Syntax Tree (AST) types
#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec::Vec,
};
use core::fmt::{self, Display};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

pub use self::data_type::{
    ArrayElemTypeDef, CharLengthUnits, CharacterLength, DataType, ExactNumberInfo, TimezoneInfo,
};
pub use self::dcl::{AlterRoleOperation, ResetConfig, RoleOption, SetConfigValue};
pub use self::ddl::{
    AlterColumnOperation, AlterIndexOperation, AlterTableOperation, ColumnDef, ColumnOption,
    ColumnOptionDef, GeneratedAs, GeneratedExpressionMode, IndexType, KeyOrIndexDisplay, Partition,
    ProcedureParam, ReferentialAction, TableConstraint, UserDefinedTypeCompositeAttributeDef,
    UserDefinedTypeRepresentation,
};
pub use self::operator::{BinaryOperator, UnaryOperator};
pub use self::query::{
    Cte, Distinct, ExceptSelectItem, ExcludeSelectItem, Fetch, ForClause, ForJson, ForXml,
    GroupByExpr, IdentWithAlias, Join, JoinConstraint, JoinOperator, JsonTableColumn,
    JsonTableColumnErrorHandling, LateralView, LockClause, LockType, NamedWindowDefinition,
    NonBlock, Offset, OffsetRows, OrderByExpr, Query, RenameSelectItem, ReplaceSelectElement,
    ReplaceSelectItem, Select, SelectInto, SelectItem, SetExpr, SetOperator, SetQuantifier, Table,
    TableAlias, TableFactor, TableVersion, TableWithJoins, Top, TopQuantity, Values,
    WildcardAdditionalOptions, With,
};
pub use self::value::{
    escape_quoted_string, DateTimeField, DollarQuotedString, TrimWhereField, Value,
};

use crate::ast::helpers::stmt_data_loading::{
    DataLoadingOptions, StageLoadSelectItem, StageParamsObject,
};
#[cfg(feature = "visitor")]
pub use visitor::*;

mod data_type;
mod dcl;
mod ddl;
pub mod helpers;
mod operator;
mod query;
mod value;

#[cfg(feature = "visitor")]
mod visitor;

struct DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<'a, T> fmt::Display for DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut delim = "";
        for t in self.slice {
            write!(f, "{delim}")?;
            delim = self.sep;
            write!(f, "{t}")?;
        }
        Ok(())
    }
}

fn display_separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep }
}

fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep: ", " }
}

/// An identifier, decomposed into its value or character data and the quote style.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Ident {
    /// The value of the identifier without quotes.
    pub value: String,
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub quote_style: Option<char>,
}

impl Ident {
    /// Create a new identifier with the given value and no quotes.
    pub fn new<S>(value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: None,
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
        }
    }
}

impl From<&str> for Ident {
    fn from(value: &str) -> Self {
        Ident {
            value: value.to_string(),
            quote_style: None,
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
pub struct ObjectName(pub Vec<Ident>);

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", display_separated(&self.0, "."))
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
            self.leading_field,
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
                if let Some(leading_field) = self.leading_field {
                    write!(f, " {leading_field}")?;
                }
                if let Some(leading_precision) = self.leading_precision {
                    write!(f, " ({leading_precision})")?;
                }
                if let Some(last_field) = self.last_field {
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

/// JsonOperator
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JsonOperator {
    /// -> keeps the value as json
    Arrow,
    /// ->> keeps the value as text or int.
    LongArrow,
    /// #> Extracts JSON sub-object at the specified path
    HashArrow,
    /// #>> Extracts JSON sub-object at the specified path as text
    HashLongArrow,
    /// : Colon is used by Snowflake (Which is similar to LongArrow)
    Colon,
    /// jsonb @> jsonb -> boolean: Test whether left json contains the right json
    AtArrow,
    /// jsonb <@ jsonb -> boolean: Test whether right json contains the left json
    ArrowAt,
    /// jsonb #- text[] -> jsonb: Deletes the field or array element at the specified
    /// path, where path elements can be either field keys or array indexes.
    HashMinus,
    /// jsonb @? jsonpath -> boolean: Does JSON path return any item for the specified
    /// JSON value?
    AtQuestion,
    /// jsonb @@ jsonpath → boolean: Returns the result of a JSON path predicate check
    /// for the specified JSON value. Only the first item of the result is taken into
    /// account. If the result is not Boolean, then NULL is returned.
    AtAt,
}

impl fmt::Display for JsonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonOperator::Arrow => {
                write!(f, "->")
            }
            JsonOperator::LongArrow => {
                write!(f, "->>")
            }
            JsonOperator::HashArrow => {
                write!(f, "#>")
            }
            JsonOperator::HashLongArrow => {
                write!(f, "#>>")
            }
            JsonOperator::Colon => {
                write!(f, ":")
            }
            JsonOperator::AtArrow => {
                write!(f, "@>")
            }
            JsonOperator::ArrowAt => write!(f, "<@"),
            JsonOperator::HashMinus => write!(f, "#-"),
            JsonOperator::AtQuestion => write!(f, "@?"),
            JsonOperator::AtAt => write!(f, "@@"),
        }
    }
}

/// A field definition within a struct.
///
/// [bigquery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct StructField {
    pub field_name: Option<Ident>,
    pub field_type: DataType,
}

impl fmt::Display for StructField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.field_name {
            write!(f, "{name} {}", self.field_type)
        } else {
            write!(f, "{}", self.field_type)
        }
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

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
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
    /// JSON access (postgres)  eg: data->'tags'
    JsonAccess {
        left: Box<Expr>,
        operator: JsonOperator,
        right: Box<Expr>,
    },
    /// CompositeAccess (postgres) eg: SELECT (information_schema._pg_expandarray(array['i','i'])).n
    CompositeAccess { expr: Box<Expr>, key: Ident },
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
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<char>,
    },
    /// `ILIKE` (case-insensitive `LIKE`)
    ILike {
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<char>,
    },
    /// SIMILAR TO regex
    SimilarTo {
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<char>,
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
    AnyOp {
        left: Box<Expr>,
        compare_op: BinaryOperator,
        right: Box<Expr>,
    },
    /// `ALL` operation e.g. `foo > ALL(bar)`, comparison operator is one of `[=, >, <, =>, =<, !=]`
    AllOp {
        left: Box<Expr>,
        compare_op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operation e.g. `NOT foo`
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// CONVERT a value to a different data type or character encoding. e.g. `CONVERT(foo USING utf8mb4)`
    Convert {
        /// The expression to convert
        expr: Box<Expr>,
        /// The target data type
        data_type: Option<DataType>,
        /// The target character encoding
        charset: Option<ObjectName>,
        /// whether the target comes before the expr (MSSQL syntax)
        target_before_value: bool,
    },
    /// `CAST` an expression to a different data type e.g. `CAST(foo AS VARCHAR(123))`
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
        // Optional CAST(string_expression AS type FORMAT format_string_expression) as used by BigQuery
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#formatting_syntax
        format: Option<CastFormat>,
    },
    /// `TRY_CAST` an expression to a different data type e.g. `TRY_CAST(foo AS VARCHAR(123))`
    //  this differs from CAST in the choice of how to implement invalid conversions
    TryCast {
        expr: Box<Expr>,
        data_type: DataType,
        // Optional CAST(string_expression AS type FORMAT format_string_expression) as used by BigQuery
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#formatting_syntax
        format: Option<CastFormat>,
    },
    /// `SAFE_CAST` an expression to a different data type e.g. `SAFE_CAST(foo AS FLOAT64)`
    //  only available for BigQuery: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#safe_casting
    //  this works the same as `TRY_CAST`
    SafeCast {
        expr: Box<Expr>,
        data_type: DataType,
        // Optional CAST(string_expression AS type FORMAT format_string_expression) as used by BigQuery
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#formatting_syntax
        format: Option<CastFormat>,
    },
    /// AT a timestamp to a different timezone e.g. `FROM_UNIXTIME(0) AT TIME ZONE 'UTC-06:00'`
    AtTimeZone {
        timestamp: Box<Expr>,
        time_zone: String,
    },
    /// Extract a field from a timestamp e.g. `EXTRACT(MONTH FROM foo)`
    ///
    /// Syntax:
    /// ```sql
    /// EXTRACT(DateTimeField FROM <expr>)
    /// ```
    Extract {
        field: DateTimeField,
        expr: Box<Expr>,
    },
    /// ```sql
    /// CEIL(<expr> [TO DateTimeField])
    /// ```
    Ceil {
        expr: Box<Expr>,
        field: DateTimeField,
    },
    /// ```sql
    /// FLOOR(<expr> [TO DateTimeField])
    /// ```
    Floor {
        expr: Box<Expr>,
        field: DateTimeField,
    },
    /// ```sql
    /// POSITION(<expr> in <expr>)
    /// ```
    Position { expr: Box<Expr>, r#in: Box<Expr> },
    /// ```sql
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    /// ```
    Substring {
        expr: Box<Expr>,
        substring_from: Option<Box<Expr>>,
        substring_for: Option<Box<Expr>>,

        // Some dialects use `SUBSTRING(expr [FROM start] [FOR len])` syntax while others omit FROM,
        // FOR keywords (e.g. Microsoft SQL Server). This flags is used for formatting.
        special: bool,
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
    Value(Value),
    /// <https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html>
    IntroducedString { introducer: String, value: Value },
    /// A constant of form `<data_type> 'value'`.
    /// This can represent ANSI SQL `DATE`, `TIME`, and `TIMESTAMP` literals (such as `DATE '2020-01-01'`),
    /// as well as constants of other types (a non-standard PostgreSQL extension).
    TypedString { data_type: DataType, value: String },
    /// Access a map-like object by field (e.g. `column['field']` or `column[4]`
    /// Note that depending on the dialect, struct like accesses may be
    /// parsed as [`ArrayIndex`](Self::ArrayIndex) or [`MapAccess`](Self::MapAccess)
    /// <https://clickhouse.com/docs/en/sql-reference/data-types/map/>
    MapAccess { column: Box<Expr>, keys: Vec<Expr> },
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function),
    /// Aggregate function with filter
    AggregateExpressionWithFilter { expr: Box<Expr>, filter: Box<Expr> },
    /// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
    ///
    /// Note we only recognize a complete single expression as `<condition>`,
    /// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
    /// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// An exists expression `[ NOT ] EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE [ NOT ] EXISTS (SELECT ...)`.
    Exists { subquery: Box<Query>, negated: bool },
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query>),
    /// An array subquery constructor, e.g. `SELECT ARRAY(SELECT 1 UNION SELECT 2)`
    ArraySubquery(Box<Query>),
    /// The `LISTAGG` function `SELECT LISTAGG(...) WITHIN GROUP (ORDER BY ...)`
    ListAgg(ListAgg),
    /// The `ARRAY_AGG` function `SELECT ARRAY_AGG(... ORDER BY ...)`
    ArrayAgg(ArrayAgg),
    /// The `GROUPING SETS` expr.
    GroupingSets(Vec<Vec<Expr>>),
    /// The `CUBE` expr.
    Cube(Vec<Vec<Expr>>),
    /// The `ROLLUP` expr.
    Rollup(Vec<Vec<Expr>>),
    /// ROW / TUPLE a single value, such as `SELECT (1, 2)`
    Tuple(Vec<Expr>),
    /// `BigQuery` specific `Struct` literal expression [1]
    /// Syntax:
    /// ```sql
    /// STRUCT<[field_name] field_type, ...>( expr1 [, ... ])
    /// ```
    /// [1]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
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
    Named { expr: Box<Expr>, name: Ident },
    /// An array index expression e.g. `(ARRAY[1, 2])[1]` or `(current_schemas(FALSE))[1]`
    ArrayIndex { obj: Box<Expr>, indexes: Vec<Expr> },
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
        columns: Vec<Ident>,
        /// `<expr>`.
        match_value: Value,
        /// `<search modifier>`
        opt_search_modifier: Option<SearchModifier>,
    },
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Identifier(s) => write!(f, "{s}"),
            Expr::MapAccess { column, keys } => {
                write!(f, "{column}")?;
                for k in keys {
                    match k {
                        k @ Expr::Value(Value::Number(_, _)) => write!(f, "[{k}]")?,
                        Expr::Value(Value::SingleQuotedString(s)) => write!(f, "[\"{s}\"]")?,
                        _ => write!(f, "[{k}]")?,
                    }
                }
                Ok(())
            }
            Expr::CompoundIdentifier(s) => write!(f, "{}", display_separated(s, ".")),
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
            } => match escape_char {
                Some(ch) => write!(
                    f,
                    "{} {}LIKE {} ESCAPE '{}'",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern,
                    ch
                ),
                _ => write!(
                    f,
                    "{} {}LIKE {}",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern
                ),
            },
            Expr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => match escape_char {
                Some(ch) => write!(
                    f,
                    "{} {}ILIKE {} ESCAPE '{}'",
                    expr,
                    if *negated { "NOT " } else { "" },
                    pattern,
                    ch
                ),
                _ => write!(
                    f,
                    "{} {}ILIKE {}",
                    expr,
                    if *negated { "NOT " } else { "" },
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
            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => match escape_char {
                Some(ch) => write!(
                    f,
                    "{} {}SIMILAR TO {} ESCAPE '{}'",
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
            } => write!(f, "{left} {compare_op} ANY({right})"),
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => write!(f, "{left} {compare_op} ALL({right})"),
            Expr::UnaryOp { op, expr } => {
                if op == &UnaryOperator::PGPostfixFactorial {
                    write!(f, "{expr}{op}")
                } else if op == &UnaryOperator::Not {
                    write!(f, "{op} {expr}")
                } else {
                    write!(f, "{op}{expr}")
                }
            }
            Expr::Convert {
                expr,
                target_before_value,
                data_type,
                charset,
            } => {
                write!(f, "CONVERT(")?;
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
                write!(f, ")")
            }
            Expr::Cast {
                expr,
                data_type,
                format,
            } => {
                if let Some(format) = format {
                    write!(f, "CAST({expr} AS {data_type} FORMAT {format})")
                } else {
                    write!(f, "CAST({expr} AS {data_type})")
                }
            }
            Expr::TryCast {
                expr,
                data_type,
                format,
            } => {
                if let Some(format) = format {
                    write!(f, "TRY_CAST({expr} AS {data_type} FORMAT {format})")
                } else {
                    write!(f, "TRY_CAST({expr} AS {data_type})")
                }
            }
            Expr::SafeCast {
                expr,
                data_type,
                format,
            } => {
                if let Some(format) = format {
                    write!(f, "SAFE_CAST({expr} AS {data_type} FORMAT {format})")
                } else {
                    write!(f, "SAFE_CAST({expr} AS {data_type})")
                }
            }
            Expr::Extract { field, expr } => write!(f, "EXTRACT({field} FROM {expr})"),
            Expr::Ceil { expr, field } => {
                if field == &DateTimeField::NoDateTime {
                    write!(f, "CEIL({expr})")
                } else {
                    write!(f, "CEIL({expr} TO {field})")
                }
            }
            Expr::Floor { expr, field } => {
                if field == &DateTimeField::NoDateTime {
                    write!(f, "FLOOR({expr})")
                } else {
                    write!(f, "FLOOR({expr} TO {field})")
                }
            }
            Expr::Position { expr, r#in } => write!(f, "POSITION({expr} IN {in})"),
            Expr::Collate { expr, collation } => write!(f, "{expr} COLLATE {collation}"),
            Expr::Nested(ast) => write!(f, "({ast})"),
            Expr::Value(v) => write!(f, "{v}"),
            Expr::IntroducedString { introducer, value } => write!(f, "{introducer} {value}"),
            Expr::TypedString { data_type, value } => {
                write!(f, "{data_type}")?;
                write!(f, " '{}'", &value::escape_single_quote_string(value))
            }
            Expr::Function(fun) => write!(f, "{fun}"),
            Expr::AggregateExpressionWithFilter { expr, filter } => {
                write!(f, "{expr} FILTER (WHERE {filter})")
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                write!(f, "CASE")?;
                if let Some(operand) = operand {
                    write!(f, " {operand}")?;
                }
                for (c, r) in conditions.iter().zip(results) {
                    write!(f, " WHEN {c} THEN {r}")?;
                }

                if let Some(else_result) = else_result {
                    write!(f, " ELSE {else_result}")?;
                }
                write!(f, " END")
            }
            Expr::Exists { subquery, negated } => write!(
                f,
                "{}EXISTS ({})",
                if *negated { "NOT " } else { "" },
                subquery
            ),
            Expr::Subquery(s) => write!(f, "({s})"),
            Expr::ArraySubquery(s) => write!(f, "ARRAY({s})"),
            Expr::ListAgg(listagg) => write!(f, "{listagg}"),
            Expr::ArrayAgg(arrayagg) => write!(f, "{arrayagg}"),
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
            } => {
                write!(f, "SUBSTRING({expr}")?;
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
                write!(f, "{} AS {}", expr, name)
            }
            Expr::ArrayIndex { obj, indexes } => {
                write!(f, "{obj}")?;
                for i in indexes {
                    write!(f, "[{i}]")?;
                }
                Ok(())
            }
            Expr::Array(set) => {
                write!(f, "{set}")
            }
            Expr::JsonAccess {
                left,
                operator,
                right,
            } => {
                if operator == &JsonOperator::Colon {
                    write!(f, "{left}{operator}{right}")
                } else {
                    write!(f, "{left} {operator} {right}")
                }
            }
            Expr::CompositeAccess { expr, key } => {
                write!(f, "{expr}.{key}")
            }
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => {
                write!(f, "{timestamp} AT TIME ZONE '{time_zone}'")
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
            WindowType::WindowSpec(spec) => write!(f, "({})", spec),
            WindowType::NamedWindow(name) => write!(f, "{}", name),
        }
    }
}

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct WindowSpec {
    /// `OVER (PARTITION BY ...)`
    pub partition_by: Vec<Expr>,
    /// `OVER (ORDER BY ...)`
    pub order_by: Vec<OrderByExpr>,
    /// `OVER (window frame)`
    pub window_frame: Option<WindowFrame>,
}

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            write!(
                f,
                "PARTITION BY {}",
                display_comma_separated(&self.partition_by)
            )?;
        }
        if !self.order_by.is_empty() {
            f.write_str(delim)?;
            delim = " ";
            write!(f, "ORDER BY {}", display_comma_separated(&self.order_by))?;
        }
        if let Some(window_frame) = &self.window_frame {
            f.write_str(delim)?;
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
}

impl fmt::Display for CommentObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CommentObject::Column => f.write_str("COLUMN"),
            CommentObject::Table => f.write_str("TABLE"),
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
    Analyze {
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
        partitions: Option<Vec<Expr>>,
        for_columns: bool,
        columns: Vec<Ident>,
        cache_metadata: bool,
        noscan: bool,
        compute_statistics: bool,
    },
    /// ```sql
    /// TRUNCATE
    /// ```
    /// Truncate (Hive)
    Truncate {
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
        partitions: Option<Vec<Expr>>,
        /// TABLE - optional keyword;
        table: bool,
    },
    /// ```sql
    /// MSCK
    /// ```
    /// Msck (Hive)
    Msck {
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
        repair: bool,
        partition_action: Option<AddDropSync>,
    },
    /// ```sql
    /// SELECT
    /// ```
    Query(Box<Query>),
    /// ```sql
    /// INSERT
    /// ```
    Insert {
        /// Only for Sqlite
        or: Option<SqliteOnConflict>,
        /// Only for mysql
        ignore: bool,
        /// INTO - optional keyword
        into: bool,
        /// TABLE
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
        /// Overwrite (Hive)
        overwrite: bool,
        /// A SQL query that specifies what to insert
        source: Option<Box<Query>>,
        /// partitioned insert (Hive)
        partitioned: Option<Vec<Expr>>,
        /// Columns defined after PARTITION
        after_columns: Vec<Ident>,
        /// whether the insert has the table keyword (Hive)
        table: bool,
        on: Option<OnInsert>,
        /// RETURNING
        returning: Option<Vec<SelectItem>>,
        /// Only for mysql
        replace_into: bool,
        /// Only for mysql
        priority: Option<MysqlInsertPriority>,
    },
    // TODO: Support ROW FORMAT
    Directory {
        overwrite: bool,
        local: bool,
        path: String,
        file_format: Option<FileFormat>,
        source: Box<Query>,
    },
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
    /// COPY INTO
    /// ```
    /// See <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table>
    /// Copy Into syntax available for Snowflake is different than the one implemented in
    /// Postgres. Although they share common prefix, it is reasonable to implement them
    /// in different enums. This can be refactored later once custom dialects
    /// are allowed to have custom Statements.
    CopyIntoSnowflake {
        into: ObjectName,
        from_stage: ObjectName,
        from_stage_alias: Option<Ident>,
        stage_params: StageParamsObject,
        from_transformations: Option<Vec<StageLoadSelectItem>>,
        files: Option<Vec<String>>,
        pattern: Option<String>,
        file_format: DataLoadingOptions,
        copy_options: DataLoadingOptions,
        validation_mode: Option<String>,
    },
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
    Update {
        /// TABLE
        table: TableWithJoins,
        /// Column assignments
        assignments: Vec<Assignment>,
        /// Table which provide value to be set
        from: Option<TableWithJoins>,
        /// WHERE
        selection: Option<Expr>,
        /// RETURNING
        returning: Option<Vec<SelectItem>>,
    },
    /// ```sql
    /// DELETE
    /// ```
    Delete {
        /// Multi tables delete are supported in mysql
        tables: Vec<ObjectName>,
        /// FROM
        from: Vec<TableWithJoins>,
        /// USING (Snowflake, Postgres, MySQL)
        using: Option<Vec<TableWithJoins>>,
        /// WHERE
        selection: Option<Expr>,
        /// RETURNING
        returning: Option<Vec<SelectItem>>,
        /// ORDER BY (MySQL)
        order_by: Vec<OrderByExpr>,
        /// LIMIT (MySQL)
        limit: Option<Expr>,
    },
    /// ```sql
    /// CREATE VIEW
    /// ```
    CreateView {
        or_replace: bool,
        materialized: bool,
        /// View name
        name: ObjectName,
        columns: Vec<Ident>,
        query: Box<Query>,
        with_options: Vec<SqlOption>,
        cluster_by: Vec<Ident>,
        /// if true, has RedShift [`WITH NO SCHEMA BINDING`] clause <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_VIEW.html>
        with_no_schema_binding: bool,
        /// if true, has SQLite `IF NOT EXISTS` clause <https://www.sqlite.org/lang_createview.html>
        if_not_exists: bool,
        /// if true, has SQLite `TEMP` or `TEMPORARY` clause <https://www.sqlite.org/lang_createview.html>
        temporary: bool,
    },
    /// ```sql
    /// CREATE TABLE
    /// ```
    CreateTable {
        or_replace: bool,
        temporary: bool,
        external: bool,
        global: Option<bool>,
        if_not_exists: bool,
        transient: bool,
        /// Table name
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        name: ObjectName,
        /// Optional schema
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        hive_distribution: HiveDistributionStyle,
        hive_formats: Option<HiveFormat>,
        table_properties: Vec<SqlOption>,
        with_options: Vec<SqlOption>,
        file_format: Option<FileFormat>,
        location: Option<String>,
        query: Option<Box<Query>>,
        without_rowid: bool,
        like: Option<ObjectName>,
        clone: Option<ObjectName>,
        engine: Option<String>,
        comment: Option<String>,
        auto_increment_offset: Option<u32>,
        default_charset: Option<String>,
        collation: Option<String>,
        on_commit: Option<OnCommit>,
        /// ClickHouse "ON CLUSTER" clause:
        /// <https://clickhouse.com/docs/en/sql-reference/distributed-ddl/>
        on_cluster: Option<String>,
        /// ClickHouse "ORDER BY " clause. Note that omitted ORDER BY is different
        /// than empty (represented as ()), the latter meaning "no sorting".
        /// <https://clickhouse.com/docs/en/sql-reference/statements/create/table/>
        order_by: Option<Vec<Ident>>,
        /// SQLite "STRICT" clause.
        /// if the "STRICT" table-option keyword is added to the end, after the closing ")",
        /// then strict typing rules apply to that table.
        strict: bool,
    },
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
    CreateIndex {
        /// index name
        name: Option<ObjectName>,
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
        using: Option<Ident>,
        columns: Vec<OrderByExpr>,
        unique: bool,
        concurrently: bool,
        if_not_exists: bool,
        include: Vec<Ident>,
        nulls_distinct: Option<bool>,
        predicate: Option<Expr>,
    },
    /// ```sql
    /// CREATE ROLE
    /// ```
    /// See [postgres](https://www.postgresql.org/docs/current/sql-createrole.html)
    CreateRole {
        names: Vec<ObjectName>,
        if_not_exists: bool,
        // Postgres
        login: Option<bool>,
        inherit: Option<bool>,
        bypassrls: Option<bool>,
        password: Option<Password>,
        superuser: Option<bool>,
        create_db: Option<bool>,
        create_role: Option<bool>,
        replication: Option<bool>,
        connection_limit: Option<Expr>,
        valid_until: Option<Expr>,
        in_role: Vec<Ident>,
        in_group: Vec<Ident>,
        role: Vec<Ident>,
        user: Vec<Ident>,
        admin: Vec<Ident>,
        // MSSQL
        authorization_owner: Option<ObjectName>,
    },
    /// ```sql
    /// ALTER TABLE
    /// ```
    AlterTable {
        /// Table name
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        name: ObjectName,
        if_exists: bool,
        only: bool,
        operations: Vec<AlterTableOperation>,
    },
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
    /// ALTER ROLE
    /// ```
    AlterRole {
        name: Ident,
        operation: AlterRoleOperation,
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
    },
    /// ```sql
    /// DROP FUNCTION
    /// ```
    DropFunction {
        if_exists: bool,
        /// One or more function to drop
        func_desc: Vec<DropFunctionDesc>,
        /// `CASCADE` or `RESTRICT`
        option: Option<ReferentialAction>,
    },
    /// ```sql
    /// DECLARE
    /// ```
    /// Declare Cursor Variables
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    Declare {
        /// Cursor name
        name: Ident,
        /// Causes the cursor to return data in binary rather than in text format.
        binary: bool,
        /// None = Not specified
        /// Some(true) = INSENSITIVE
        /// Some(false) = ASENSITIVE
        sensitive: Option<bool>,
        /// None = Not specified
        /// Some(true) = SCROLL
        /// Some(false) = NO SCROLL
        scroll: Option<bool>,
        /// None = Not specified
        /// Some(true) = WITH HOLD, specifies that the cursor can continue to be used after the transaction that created it successfully commits
        /// Some(false) = WITHOUT HOLD, specifies that the cursor cannot be used outside of the transaction that created it
        hold: Option<bool>,
        query: Box<Query>,
    },
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
        /// Optional, It's possible to fetch rows form cursor to the table
        into: Option<ObjectName>,
    },
    /// ```sql
    /// DISCARD [ ALL | PLANS | SEQUENCES | TEMPORARY | TEMP ]
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statement,
    /// but may also compatible with other SQL.
    Discard { object_type: DiscardObject },
    /// ```sql
    /// SET [ SESSION | LOCAL ] ROLE role_name
    /// ```
    ///
    /// Sets sesssion state. Examples: [ANSI][1], [Postgresql][2], [MySQL][3], and [Oracle][4]
    ///
    /// [1]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#set-role-statement
    /// [2]: https://www.postgresql.org/docs/14/sql-set-role.html
    /// [3]: https://dev.mysql.com/doc/refman/8.0/en/set-role.html
    /// [4]: https://docs.oracle.com/cd/B19306_01/server.102/b14200/statements_10004.htm
    SetRole {
        /// Non-ANSI optional identifier to inform if the role is defined inside the current session (`SESSION`) or transaction (`LOCAL`).
        context_modifier: ContextModifier,
        /// Role name. If NONE is specified, then the current role name is removed.
        role_name: Option<Ident>,
    },
    /// ```sql
    /// SET <variable>
    /// ```
    ///
    /// Note: this is not a standard SQL statement, but it is supported by at
    /// least MySQL and PostgreSQL. Not all MySQL-specific syntatic forms are
    /// supported yet.
    SetVariable {
        local: bool,
        hivevar: bool,
        variable: ObjectName,
        value: Vec<Expr>,
    },
    /// ```sql
    /// SET TIME ZONE <value>
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statements
    /// `SET TIME ZONE <value>` is an alias for `SET timezone TO <value>` in PostgreSQL
    SetTimeZone { local: bool, value: Expr },
    /// ```sql
    /// SET NAMES 'charset_name' [COLLATE 'collation_name']
    /// ```
    ///
    /// Note: this is a MySQL-specific statement.
    SetNames {
        charset_name: String,
        collation_name: Option<String>,
    },
    /// ```sql
    /// SET NAMES DEFAULT
    /// ```
    ///
    /// Note: this is a MySQL-specific statement.
    SetNamesDefault {},
    /// `SHOW FUNCTIONS`
    ///
    /// Note: this is a Presto-specific statement.
    ShowFunctions { filter: Option<ShowStatementFilter> },
    /// ```sql
    /// SHOW <variable>
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statement.
    ShowVariable { variable: Vec<Ident> },
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
    ///
    /// Note: this is a MySQL-specific statement.
    ShowColumns {
        extended: bool,
        full: bool,
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
        filter: Option<ShowStatementFilter>,
    },
    /// ```sql
    /// SHOW TABLES
    /// ```
    /// Note: this is a MySQL-specific statement.
    ShowTables {
        extended: bool,
        full: bool,
        db_name: Option<Ident>,
        filter: Option<ShowStatementFilter>,
    },
    /// ```sql
    /// SHOW COLLATION
    /// ```
    ///
    /// Note: this is a MySQL-specific statement.
    ShowCollation { filter: Option<ShowStatementFilter> },
    /// ```sql
    /// USE
    /// ```
    ///
    /// Note: This is a MySQL-specific statement.
    Use { db_name: Ident },
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
        /// Only for SQLite
        modifier: Option<TransactionModifier>,
    },
    /// ```sql
    /// SET TRANSACTION ...
    /// ```
    SetTransaction {
        modes: Vec<TransactionMode>,
        snapshot: Option<Value>,
        session: bool,
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
    Commit { chain: bool },
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
    },
    /// ```sql
    /// CREATE DATABASE
    /// ```
    CreateDatabase {
        db_name: ObjectName,
        if_not_exists: bool,
        location: Option<String>,
        managed_location: Option<String>,
    },
    /// ```sql
    /// CREATE FUNCTION
    /// ```
    ///
    /// Supported variants:
    /// 1. [Hive](https://cwiki.apache.org/confluence/display/hive/languagemanual+ddl#LanguageManualDDL-Create/Drop/ReloadFunction)
    /// 2. [Postgres](https://www.postgresql.org/docs/15/sql-createfunction.html)
    CreateFunction {
        or_replace: bool,
        temporary: bool,
        name: ObjectName,
        args: Option<Vec<OperateFunctionArg>>,
        return_type: Option<DataType>,
        /// Optional parameters.
        params: CreateFunctionBody,
    },
    /// ```sql
    /// CREATE PROCEDURE
    /// ```
    CreateProcedure {
        or_alter: bool,
        name: ObjectName,
        params: Option<Vec<ProcedureParam>>,
        body: Vec<Statement>,
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
        directory_table_params: DataLoadingOptions,
        file_format: DataLoadingOptions,
        copy_options: DataLoadingOptions,
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
        objects: GrantObjects,
        grantees: Vec<Ident>,
        with_grant_option: bool,
        granted_by: Option<Ident>,
    },
    /// ```sql
    /// REVOKE privileges ON objects FROM grantees
    /// ```
    Revoke {
        privileges: Privileges,
        objects: GrantObjects,
        grantees: Vec<Ident>,
        granted_by: Option<Ident>,
        cascade: bool,
    },
    /// ```sql
    /// DEALLOCATE [ PREPARE ] { name | ALL }
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Deallocate { name: Ident, prepare: bool },
    /// ```sql
    /// EXECUTE name [ ( parameter [, ...] ) ]
    /// ```
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Execute { name: Ident, parameters: Vec<Expr> },
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
    /// See <https://clickhouse.com/docs/ru/sql-reference/statements/kill/>
    /// See <https://dev.mysql.com/doc/refman/8.0/en/kill.html>
    Kill {
        modifier: Option<KillType>,
        // processlist_id
        id: u64,
    },
    /// ```sql
    /// EXPLAIN TABLE
    /// ```
    /// Note: this is a MySQL-specific statement. See <https://dev.mysql.com/doc/refman/8.0/en/explain.html>
    ExplainTable {
        /// If true, query used the MySQL `DESCRIBE` alias for explain
        describe_alias: bool,
        /// Table name
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        table_name: ObjectName,
    },
    /// ```sql
    /// [EXPLAIN | DESCRIBE <select statement>
    /// ```
    Explain {
        // If true, query used the MySQL `DESCRIBE` alias for explain
        describe_alias: bool,
        /// Carry out the command and show actual run times and other statistics.
        analyze: bool,
        // Display additional information regarding the plan.
        verbose: bool,
        /// A SQL query that specifies what to explain
        statement: Box<Statement>,
        /// Optional output format of explain
        format: Option<AnalyzeFormat>,
    },
    /// ```sql
    /// SAVEPOINT
    /// ```
    /// Define a new savepoint within the current transaction
    Savepoint { name: Ident },
    /// ```sql
    /// RELEASE [ SAVEPOINT ] savepoint_name
    /// ```
    ReleaseSavepoint { name: Ident },
    /// ```sql
    /// MERGE INTO <statement>
    /// ```
    /// Based on Snowflake. See <https://docs.snowflake.com/en/sql-reference/sql/merge.html>
    Merge {
        // optional INTO keyword
        into: bool,
        // Specifies the table to merge
        table: TableFactor,
        // Specifies the table or subquery to join with the target table
        source: TableFactor,
        // Specifies the expression on which to join the target table and source
        on: Box<Expr>,
        // Specifies the actions to perform when values match or do not match.
        clauses: Vec<MergeClause>,
    },
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
        query: Option<Query>,
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
    /// ```sql
    /// CREATE TYPE <name>
    /// ```
    CreateType {
        name: ObjectName,
        representation: UserDefinedTypeRepresentation,
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
    LockTables { tables: Vec<LockTable> },
    /// ```sql
    /// UNLOCK TABLES
    /// ```
    /// Note: this is a MySQL-specific statement. See <https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html>
    UnlockTables,
}

impl fmt::Display for Statement {
    // Clippy thinks this function is too complicated, but it is painful to
    // split up without extracting structs for each `Statement` variant.
    #[allow(clippy::cognitive_complexity)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::Kill { modifier, id } => {
                write!(f, "KILL ")?;

                if let Some(m) = modifier {
                    write!(f, "{m} ")?;
                }

                write!(f, "{id}")
            }
            Statement::ExplainTable {
                describe_alias,
                table_name,
            } => {
                if *describe_alias {
                    write!(f, "DESCRIBE ")?;
                } else {
                    write!(f, "EXPLAIN ")?;
                }

                write!(f, "{table_name}")
            }
            Statement::Explain {
                describe_alias,
                verbose,
                analyze,
                statement,
                format,
            } => {
                if *describe_alias {
                    write!(f, "DESCRIBE ")?;
                } else {
                    write!(f, "EXPLAIN ")?;
                }

                if *analyze {
                    write!(f, "ANALYZE ")?;
                }

                if *verbose {
                    write!(f, "VERBOSE ")?;
                }

                if let Some(format) = format {
                    write!(f, "FORMAT {format} ")?;
                }

                write!(f, "{statement}")
            }
            Statement::Query(s) => write!(f, "{s}"),
            Statement::Declare {
                name,
                binary,
                sensitive,
                scroll,
                hold,
                query,
            } => {
                write!(f, "DECLARE {name} ")?;

                if *binary {
                    write!(f, "BINARY ")?;
                }

                if let Some(sensitive) = sensitive {
                    if *sensitive {
                        write!(f, "INSENSITIVE ")?;
                    } else {
                        write!(f, "ASENSITIVE ")?;
                    }
                }

                if let Some(scroll) = scroll {
                    if *scroll {
                        write!(f, "SCROLL ")?;
                    } else {
                        write!(f, "NO SCROLL ")?;
                    }
                }

                write!(f, "CURSOR ")?;

                if let Some(hold) = hold {
                    if *hold {
                        write!(f, "WITH HOLD ")?;
                    } else {
                        write!(f, "WITHOUT HOLD ")?;
                    }
                }

                write!(f, "FOR {query}")
            }
            Statement::Fetch {
                name,
                direction,
                into,
            } => {
                write!(f, "FETCH {direction} ")?;

                write!(f, "IN {name}")?;

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
            Statement::Msck {
                table_name,
                repair,
                partition_action,
            } => {
                write!(
                    f,
                    "MSCK {repair}TABLE {table}",
                    repair = if *repair { "REPAIR " } else { "" },
                    table = table_name
                )?;
                if let Some(pa) = partition_action {
                    write!(f, " {pa}")?;
                }
                Ok(())
            }
            Statement::Truncate {
                table_name,
                partitions,
                table,
            } => {
                let table = if *table { "TABLE " } else { "" };
                write!(f, "TRUNCATE {table}{table_name}")?;
                if let Some(ref parts) = partitions {
                    if !parts.is_empty() {
                        write!(f, " PARTITION ({})", display_comma_separated(parts))?;
                    }
                }
                Ok(())
            }
            Statement::AttachDatabase {
                schema_name,
                database_file_name,
                database,
            } => {
                let keyword = if *database { "DATABASE " } else { "" };
                write!(f, "ATTACH {keyword}{database_file_name} AS {schema_name}")
            }
            Statement::Analyze {
                table_name,
                partitions,
                for_columns,
                columns,
                cache_metadata,
                noscan,
                compute_statistics,
            } => {
                write!(f, "ANALYZE TABLE {table_name}")?;
                if let Some(ref parts) = partitions {
                    if !parts.is_empty() {
                        write!(f, " PARTITION ({})", display_comma_separated(parts))?;
                    }
                }

                if *compute_statistics {
                    write!(f, " COMPUTE STATISTICS")?;
                }
                if *noscan {
                    write!(f, " NOSCAN")?;
                }
                if *cache_metadata {
                    write!(f, " CACHE METADATA")?;
                }
                if *for_columns {
                    write!(f, " FOR COLUMNS")?;
                    if !columns.is_empty() {
                        write!(f, " {}", display_comma_separated(columns))?;
                    }
                }
                Ok(())
            }
            Statement::Insert {
                or,
                ignore,
                into,
                table_name,
                overwrite,
                partitioned,
                columns,
                after_columns,
                source,
                table,
                on,
                returning,
                replace_into,
                priority,
            } => {
                if let Some(action) = or {
                    write!(f, "INSERT OR {action} INTO {table_name} ")?;
                } else {
                    write!(
                        f,
                        "{start}",
                        start = if *replace_into { "REPLACE" } else { "INSERT" },
                    )?;
                    if let Some(priority) = priority {
                        write!(f, " {priority}",)?;
                    }

                    write!(
                        f,
                        "{ignore}{over}{int}{tbl} {table_name} ",
                        table_name = table_name,
                        ignore = if *ignore { " IGNORE" } else { "" },
                        over = if *overwrite { " OVERWRITE" } else { "" },
                        int = if *into { " INTO" } else { "" },
                        tbl = if *table { " TABLE" } else { "" },
                    )?;
                }
                if !columns.is_empty() {
                    write!(f, "({}) ", display_comma_separated(columns))?;
                }
                if let Some(ref parts) = partitioned {
                    if !parts.is_empty() {
                        write!(f, "PARTITION ({}) ", display_comma_separated(parts))?;
                    }
                }
                if !after_columns.is_empty() {
                    write!(f, "({}) ", display_comma_separated(after_columns))?;
                }

                if let Some(source) = source {
                    write!(f, "{source}")?;
                }

                if source.is_none() && columns.is_empty() {
                    write!(f, "DEFAULT VALUES")?;
                }

                if let Some(on) = on {
                    write!(f, "{on}")?;
                }

                if let Some(returning) = returning {
                    write!(f, " RETURNING {}", display_comma_separated(returning))?;
                }

                Ok(())
            }

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
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning,
            } => {
                write!(f, "UPDATE {table}")?;
                if !assignments.is_empty() {
                    write!(f, " SET {}", display_comma_separated(assignments))?;
                }
                if let Some(from) = from {
                    write!(f, " FROM {from}")?;
                }
                if let Some(selection) = selection {
                    write!(f, " WHERE {selection}")?;
                }
                if let Some(returning) = returning {
                    write!(f, " RETURNING {}", display_comma_separated(returning))?;
                }
                Ok(())
            }
            Statement::Delete {
                tables,
                from,
                using,
                selection,
                returning,
                order_by,
                limit,
            } => {
                write!(f, "DELETE ")?;
                if !tables.is_empty() {
                    write!(f, "{} ", display_comma_separated(tables))?;
                }
                write!(f, "FROM {}", display_comma_separated(from))?;
                if let Some(using) = using {
                    write!(f, " USING {}", display_comma_separated(using))?;
                }
                if let Some(selection) = selection {
                    write!(f, " WHERE {selection}")?;
                }
                if let Some(returning) = returning {
                    write!(f, " RETURNING {}", display_comma_separated(returning))?;
                }
                if !order_by.is_empty() {
                    write!(f, " ORDER BY {}", display_comma_separated(order_by))?;
                }
                if let Some(limit) = limit {
                    write!(f, " LIMIT {limit}")?;
                }
                Ok(())
            }
            Statement::Close { cursor } => {
                write!(f, "CLOSE {cursor}")?;

                Ok(())
            }
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                location,
                managed_location,
            } => {
                write!(f, "CREATE DATABASE")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {db_name}")?;
                if let Some(l) = location {
                    write!(f, " LOCATION '{l}'")?;
                }
                if let Some(ml) = managed_location {
                    write!(f, " MANAGEDLOCATION '{ml}'")?;
                }
                Ok(())
            }
            Statement::CreateFunction {
                or_replace,
                temporary,
                name,
                args,
                return_type,
                params,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}{temp}FUNCTION {name}",
                    temp = if *temporary { "TEMPORARY " } else { "" },
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                )?;
                if let Some(args) = args {
                    write!(f, "({})", display_comma_separated(args))?;
                }
                if let Some(return_type) = return_type {
                    write!(f, " RETURNS {return_type}")?;
                }
                write!(f, "{params}")?;
                Ok(())
            }
            Statement::CreateProcedure {
                name,
                or_alter,
                params,
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
                write!(
                    f,
                    " AS BEGIN {body} END",
                    body = display_separated(body, "; ")
                )
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
            Statement::CreateView {
                name,
                or_replace,
                columns,
                query,
                materialized,
                with_options,
                cluster_by,
                with_no_schema_binding,
                if_not_exists,
                temporary,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}{materialized}{temporary}VIEW {if_not_exists}{name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    materialized = if *materialized { "MATERIALIZED " } else { "" },
                    name = name,
                    temporary = if *temporary { "TEMPORARY " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" }
                )?;
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
                if !cluster_by.is_empty() {
                    write!(f, " CLUSTER BY ({})", display_comma_separated(cluster_by))?;
                }
                write!(f, " AS {query}")?;
                if *with_no_schema_binding {
                    write!(f, " WITH NO SCHEMA BINDING")?;
                }
                Ok(())
            }
            Statement::CreateTable {
                name,
                columns,
                constraints,
                table_properties,
                with_options,
                or_replace,
                if_not_exists,
                transient,
                hive_distribution,
                hive_formats,
                external,
                global,
                temporary,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                default_charset,
                engine,
                comment,
                auto_increment_offset,
                collation,
                on_commit,
                on_cluster,
                order_by,
                strict,
            } => {
                // We want to allow the following options
                // Empty column list, allowed by PostgreSQL:
                //   `CREATE TABLE t ()`
                // No columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t AS SELECT a from t2`
                // Columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t (a INT) AS SELECT a from t2`
                write!(
                    f,
                    "CREATE {or_replace}{external}{global}{temporary}{transient}TABLE {if_not_exists}{name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    external = if *external { "EXTERNAL " } else { "" },
                    global = global
                        .map(|global| {
                            if global {
                                "GLOBAL "
                            } else {
                                "LOCAL "
                            }
                        })
                        .unwrap_or(""),
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    temporary = if *temporary { "TEMPORARY " } else { "" },
                    transient = if *transient { "TRANSIENT " } else { "" },
                    name = name,
                )?;
                if let Some(on_cluster) = on_cluster {
                    write!(
                        f,
                        " ON CLUSTER {}",
                        on_cluster.replace('{', "'{").replace('}', "}'")
                    )?;
                }
                if !columns.is_empty() || !constraints.is_empty() {
                    write!(f, " ({}", display_comma_separated(columns))?;
                    if !columns.is_empty() && !constraints.is_empty() {
                        write!(f, ", ")?;
                    }
                    write!(f, "{})", display_comma_separated(constraints))?;
                } else if query.is_none() && like.is_none() && clone.is_none() {
                    // PostgreSQL allows `CREATE TABLE t ();`, but requires empty parens
                    write!(f, " ()")?;
                }
                // Only for SQLite
                if *without_rowid {
                    write!(f, " WITHOUT ROWID")?;
                }

                // Only for Hive
                if let Some(l) = like {
                    write!(f, " LIKE {l}")?;
                }

                if let Some(c) = clone {
                    write!(f, " CLONE {c}")?;
                }

                match hive_distribution {
                    HiveDistributionStyle::PARTITIONED { columns } => {
                        write!(f, " PARTITIONED BY ({})", display_comma_separated(columns))?;
                    }
                    HiveDistributionStyle::CLUSTERED {
                        columns,
                        sorted_by,
                        num_buckets,
                    } => {
                        write!(f, " CLUSTERED BY ({})", display_comma_separated(columns))?;
                        if !sorted_by.is_empty() {
                            write!(f, " SORTED BY ({})", display_comma_separated(sorted_by))?;
                        }
                        if *num_buckets > 0 {
                            write!(f, " INTO {num_buckets} BUCKETS")?;
                        }
                    }
                    HiveDistributionStyle::SKEWED {
                        columns,
                        on,
                        stored_as_directories,
                    } => {
                        write!(
                            f,
                            " SKEWED BY ({})) ON ({})",
                            display_comma_separated(columns),
                            display_comma_separated(on)
                        )?;
                        if *stored_as_directories {
                            write!(f, " STORED AS DIRECTORIES")?;
                        }
                    }
                    _ => (),
                }

                if let Some(HiveFormat {
                    row_format,
                    storage,
                    location,
                }) = hive_formats
                {
                    match row_format {
                        Some(HiveRowFormat::SERDE { class }) => {
                            write!(f, " ROW FORMAT SERDE '{class}'")?
                        }
                        Some(HiveRowFormat::DELIMITED) => write!(f, " ROW FORMAT DELIMITED")?,
                        None => (),
                    }
                    match storage {
                        Some(HiveIOFormat::IOF {
                            input_format,
                            output_format,
                        }) => write!(
                            f,
                            " STORED AS INPUTFORMAT {input_format} OUTPUTFORMAT {output_format}"
                        )?,
                        Some(HiveIOFormat::FileFormat { format }) if !*external => {
                            write!(f, " STORED AS {format}")?
                        }
                        _ => (),
                    }
                    if !*external {
                        if let Some(loc) = location {
                            write!(f, " LOCATION '{loc}'")?;
                        }
                    }
                }
                if *external {
                    write!(
                        f,
                        " STORED AS {} LOCATION '{}'",
                        file_format.as_ref().unwrap(),
                        location.as_ref().unwrap()
                    )?;
                }
                if !table_properties.is_empty() {
                    write!(
                        f,
                        " TBLPROPERTIES ({})",
                        display_comma_separated(table_properties)
                    )?;
                }
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                if let Some(engine) = engine {
                    write!(f, " ENGINE={engine}")?;
                }
                if let Some(comment) = comment {
                    write!(f, " COMMENT '{comment}'")?;
                }
                if let Some(auto_increment_offset) = auto_increment_offset {
                    write!(f, " AUTO_INCREMENT {auto_increment_offset}")?;
                }
                if let Some(order_by) = order_by {
                    write!(f, " ORDER BY ({})", display_comma_separated(order_by))?;
                }
                if let Some(query) = query {
                    write!(f, " AS {query}")?;
                }
                if let Some(default_charset) = default_charset {
                    write!(f, " DEFAULT CHARSET={default_charset}")?;
                }
                if let Some(collation) = collation {
                    write!(f, " COLLATE={collation}")?;
                }

                if on_commit.is_some() {
                    let on_commit = match on_commit {
                        Some(OnCommit::DeleteRows) => "ON COMMIT DELETE ROWS",
                        Some(OnCommit::PreserveRows) => "ON COMMIT PRESERVE ROWS",
                        Some(OnCommit::Drop) => "ON COMMIT DROP",
                        None => "",
                    };
                    write!(f, " {on_commit}")?;
                }
                if *strict {
                    write!(f, " STRICT")?;
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
            Statement::CreateIndex {
                name,
                table_name,
                using,
                columns,
                unique,
                concurrently,
                if_not_exists,
                include,
                nulls_distinct,
                predicate,
            } => {
                write!(
                    f,
                    "CREATE {unique}INDEX {concurrently}{if_not_exists}",
                    unique = if *unique { "UNIQUE " } else { "" },
                    concurrently = if *concurrently { "CONCURRENTLY " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                )?;
                if let Some(value) = name {
                    write!(f, "{value} ")?;
                }
                write!(f, "ON {table_name}")?;
                if let Some(value) = using {
                    write!(f, " USING {value} ")?;
                }
                write!(f, "({})", display_separated(columns, ","))?;
                if !include.is_empty() {
                    write!(f, " INCLUDE ({})", display_separated(include, ","))?;
                }
                if let Some(value) = nulls_distinct {
                    if *value {
                        write!(f, " NULLS DISTINCT")?;
                    } else {
                        write!(f, " NULLS NOT DISTINCT")?;
                    }
                }
                if let Some(predicate) = predicate {
                    write!(f, " WHERE {predicate}")?;
                }
                Ok(())
            }
            Statement::CreateRole {
                names,
                if_not_exists,
                inherit,
                login,
                bypassrls,
                password,
                create_db,
                create_role,
                superuser,
                replication,
                connection_limit,
                valid_until,
                in_role,
                in_group,
                role,
                user,
                admin,
                authorization_owner,
            } => {
                write!(
                    f,
                    "CREATE ROLE {if_not_exists}{names}{superuser}{create_db}{create_role}{inherit}{login}{replication}{bypassrls}",
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    names = display_separated(names, ", "),
                    superuser = match *superuser {
                        Some(true) => " SUPERUSER",
                        Some(false) => " NOSUPERUSER",
                        None => ""
                    },
                    create_db = match *create_db {
                        Some(true) => " CREATEDB",
                        Some(false) => " NOCREATEDB",
                        None => ""
                    },
                    create_role = match *create_role {
                        Some(true) => " CREATEROLE",
                        Some(false) => " NOCREATEROLE",
                        None => ""
                    },
                    inherit = match *inherit {
                        Some(true) => " INHERIT",
                        Some(false) => " NOINHERIT",
                        None => ""
                    },
                    login = match *login {
                        Some(true) => " LOGIN",
                        Some(false) => " NOLOGIN",
                        None => ""
                    },
                    replication = match *replication {
                        Some(true) => " REPLICATION",
                        Some(false) => " NOREPLICATION",
                        None => ""
                    },
                    bypassrls = match *bypassrls {
                        Some(true) => " BYPASSRLS",
                        Some(false) => " NOBYPASSRLS",
                        None => ""
                    }
                )?;
                if let Some(limit) = connection_limit {
                    write!(f, " CONNECTION LIMIT {limit}")?;
                }
                match password {
                    Some(Password::Password(pass)) => write!(f, " PASSWORD {pass}"),
                    Some(Password::NullPassword) => write!(f, " PASSWORD NULL"),
                    None => Ok(()),
                }?;
                if let Some(until) = valid_until {
                    write!(f, " VALID UNTIL {until}")?;
                }
                if !in_role.is_empty() {
                    write!(f, " IN ROLE {}", display_comma_separated(in_role))?;
                }
                if !in_group.is_empty() {
                    write!(f, " IN GROUP {}", display_comma_separated(in_group))?;
                }
                if !role.is_empty() {
                    write!(f, " ROLE {}", display_comma_separated(role))?;
                }
                if !user.is_empty() {
                    write!(f, " USER {}", display_comma_separated(user))?;
                }
                if !admin.is_empty() {
                    write!(f, " ADMIN {}", display_comma_separated(admin))?;
                }
                if let Some(owner) = authorization_owner {
                    write!(f, " AUTHORIZATION {owner}")?;
                }
                Ok(())
            }
            Statement::AlterTable {
                name,
                if_exists,
                only,
                operations,
            } => {
                write!(f, "ALTER TABLE ")?;
                if *if_exists {
                    write!(f, "IF EXISTS ")?;
                }
                if *only {
                    write!(f, "ONLY ")?;
                }
                write!(
                    f,
                    "{name} {operations}",
                    operations = display_comma_separated(operations)
                )
            }
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
            Statement::AlterRole { name, operation } => {
                write!(f, "ALTER ROLE {name} {operation}")
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
            } => write!(
                f,
                "DROP {}{}{} {}{}{}{}",
                if *temporary { "TEMPORARY " } else { "" },
                object_type,
                if *if_exists { " IF EXISTS" } else { "" },
                display_comma_separated(names),
                if *cascade { " CASCADE" } else { "" },
                if *restrict { " RESTRICT" } else { "" },
                if *purge { " PURGE" } else { "" }
            ),
            Statement::DropFunction {
                if_exists,
                func_desc,
                option,
            } => {
                write!(
                    f,
                    "DROP FUNCTION{} {}",
                    if *if_exists { " IF EXISTS" } else { "" },
                    display_comma_separated(func_desc),
                )?;
                if let Some(op) = option {
                    write!(f, " {op}")?;
                }
                Ok(())
            }
            Statement::Discard { object_type } => {
                write!(f, "DISCARD {object_type}")?;
                Ok(())
            }
            Self::SetRole {
                context_modifier,
                role_name,
            } => {
                let role_name = role_name.clone().unwrap_or_else(|| Ident::new("NONE"));
                write!(f, "SET{context_modifier} ROLE {role_name}")
            }
            Statement::SetVariable {
                local,
                variable,
                hivevar,
                value,
            } => {
                f.write_str("SET ")?;
                if *local {
                    f.write_str("LOCAL ")?;
                }
                write!(
                    f,
                    "{hivevar}{name} = {value}",
                    hivevar = if *hivevar { "HIVEVAR:" } else { "" },
                    name = variable,
                    value = display_comma_separated(value)
                )
            }
            Statement::SetTimeZone { local, value } => {
                f.write_str("SET ")?;
                if *local {
                    f.write_str("LOCAL ")?;
                }
                write!(f, "TIME ZONE {value}")
            }
            Statement::SetNames {
                charset_name,
                collation_name,
            } => {
                f.write_str("SET NAMES ")?;
                f.write_str(charset_name)?;

                if let Some(collation) = collation_name {
                    f.write_str(" COLLATE ")?;
                    f.write_str(collation)?;
                };

                Ok(())
            }
            Statement::SetNamesDefault {} => {
                f.write_str("SET NAMES DEFAULT")?;

                Ok(())
            }
            Statement::ShowVariable { variable } => {
                write!(f, "SHOW")?;
                if !variable.is_empty() {
                    write!(f, " {}", display_separated(variable, " "))?;
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
                table_name,
                filter,
            } => {
                write!(
                    f,
                    "SHOW {extended}{full}COLUMNS FROM {table_name}",
                    extended = if *extended { "EXTENDED " } else { "" },
                    full = if *full { "FULL " } else { "" },
                    table_name = table_name,
                )?;
                if let Some(filter) = filter {
                    write!(f, " {filter}")?;
                }
                Ok(())
            }
            Statement::ShowTables {
                extended,
                full,
                db_name,
                filter,
            } => {
                write!(
                    f,
                    "SHOW {extended}{full}TABLES",
                    extended = if *extended { "EXTENDED " } else { "" },
                    full = if *full { "FULL " } else { "" },
                )?;
                if let Some(db_name) = db_name {
                    write!(f, " FROM {db_name}")?;
                }
                if let Some(filter) = filter {
                    write!(f, " {filter}")?;
                }
                Ok(())
            }
            Statement::ShowFunctions { filter } => {
                write!(f, "SHOW FUNCTIONS")?;
                if let Some(filter) = filter {
                    write!(f, " {filter}")?;
                }
                Ok(())
            }
            Statement::Use { db_name } => {
                write!(f, "USE {db_name}")?;
                Ok(())
            }
            Statement::ShowCollation { filter } => {
                write!(f, "SHOW COLLATION")?;
                if let Some(filter) = filter {
                    write!(f, " {filter}")?;
                }
                Ok(())
            }
            Statement::StartTransaction {
                modes,
                begin: syntax_begin,
                modifier,
            } => {
                if *syntax_begin {
                    if let Some(modifier) = *modifier {
                        write!(f, "BEGIN {} TRANSACTION", modifier)?;
                    } else {
                        write!(f, "BEGIN TRANSACTION")?;
                    }
                } else {
                    write!(f, "START TRANSACTION")?;
                }
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                Ok(())
            }
            Statement::SetTransaction {
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
            Statement::Commit { chain } => {
                write!(f, "COMMIT{}", if *chain { " AND CHAIN" } else { "" },)
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
            } => write!(
                f,
                "CREATE SCHEMA {if_not_exists}{name}",
                if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                name = schema_name
            ),
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
                granted_by,
            } => {
                write!(f, "GRANT {privileges} ")?;
                write!(f, "ON {objects} ")?;
                write!(f, "TO {}", display_comma_separated(grantees))?;
                if *with_grant_option {
                    write!(f, " WITH GRANT OPTION")?;
                }
                if let Some(grantor) = granted_by {
                    write!(f, " GRANTED BY {grantor}")?;
                }
                Ok(())
            }
            Statement::Revoke {
                privileges,
                objects,
                grantees,
                granted_by,
                cascade,
            } => {
                write!(f, "REVOKE {privileges} ")?;
                write!(f, "ON {objects} ")?;
                write!(f, "FROM {}", display_comma_separated(grantees))?;
                if let Some(grantor) = granted_by {
                    write!(f, " GRANTED BY {grantor}")?;
                }
                write!(f, " {}", if *cascade { "CASCADE" } else { "RESTRICT" })?;
                Ok(())
            }
            Statement::Deallocate { name, prepare } => write!(
                f,
                "DEALLOCATE {prepare}{name}",
                prepare = if *prepare { "PREPARE " } else { "" },
                name = name,
            ),
            Statement::Execute { name, parameters } => {
                write!(f, "EXECUTE {name}")?;
                if !parameters.is_empty() {
                    write!(f, "({})", display_comma_separated(parameters))?;
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
            Statement::Merge {
                into,
                table,
                source,
                on,
                clauses,
            } => {
                write!(
                    f,
                    "MERGE{int} {table} USING {source} ",
                    int = if *into { " INTO" } else { "" }
                )?;
                write!(f, "ON {on} ")?;
                write!(f, "{}", display_separated(clauses, " "))
            }
            Statement::Cache {
                table_name,
                table_flag,
                has_as,
                options,
                query,
            } => {
                if table_flag.is_some() {
                    write!(
                        f,
                        "CACHE {table_flag} TABLE {table_name}",
                        table_flag = table_flag.clone().unwrap(),
                        table_name = table_name,
                    )?;
                } else {
                    write!(f, "CACHE TABLE {table_name}",)?;
                }

                if !options.is_empty() {
                    write!(f, " OPTIONS({})", display_comma_separated(options))?;
                }

                let has_query = query.is_some();
                if *has_as && has_query {
                    write!(f, " AS {query}", query = query.clone().unwrap())
                } else if !has_as && has_query {
                    write!(f, " {query}", query = query.clone().unwrap())
                } else if *has_as && !has_query {
                    write!(f, " AS")
                } else {
                    Ok(())
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
                    write!(f, " DIRECTORY=({})", directory_table_params)?;
                }
                if !file_format.options.is_empty() {
                    write!(f, " FILE_FORMAT=({})", file_format)?;
                }
                if !copy_options.options.is_empty() {
                    write!(f, " COPY_OPTIONS=({})", copy_options)?;
                }
                if comment.is_some() {
                    write!(f, " COMMENT='{}'", comment.as_ref().unwrap())?;
                }
                Ok(())
            }
            Statement::CopyIntoSnowflake {
                into,
                from_stage,
                from_stage_alias,
                stage_params,
                from_transformations,
                files,
                pattern,
                file_format,
                copy_options,
                validation_mode,
            } => {
                write!(f, "COPY INTO {}", into)?;
                if from_transformations.is_none() {
                    // Standard data load
                    write!(f, " FROM {}{}", from_stage, stage_params)?;
                    if from_stage_alias.as_ref().is_some() {
                        write!(f, " AS {}", from_stage_alias.as_ref().unwrap())?;
                    }
                } else {
                    // Data load with transformation
                    write!(
                        f,
                        " FROM (SELECT {} FROM {}{}",
                        display_separated(from_transformations.as_ref().unwrap(), ", "),
                        from_stage,
                        stage_params,
                    )?;
                    if from_stage_alias.as_ref().is_some() {
                        write!(f, " AS {}", from_stage_alias.as_ref().unwrap())?;
                    }
                    write!(f, ")")?;
                }
                if files.is_some() {
                    write!(
                        f,
                        " FILES = ('{}')",
                        display_separated(files.as_ref().unwrap(), "', '")
                    )?;
                }
                if pattern.is_some() {
                    write!(f, " PATTERN = '{}'", pattern.as_ref().unwrap())?;
                }
                if !file_format.options.is_empty() {
                    write!(f, " FILE_FORMAT=({})", file_format)?;
                }
                if !copy_options.options.is_empty() {
                    write!(f, " COPY_OPTIONS=({})", copy_options)?;
                }
                if validation_mode.is_some() {
                    write!(
                        f,
                        " VALIDATION_MODE = {}",
                        validation_mode.as_ref().unwrap()
                    )?;
                }
                Ok(())
            }
            Statement::CreateType {
                name,
                representation,
            } => {
                write!(f, "CREATE TYPE {name} AS {representation}")
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
    MinValue(MinMaxValue),
    MaxValue(MinMaxValue),
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
            SequenceOptions::MinValue(value) => match value {
                MinMaxValue::Empty => {
                    write!(f, "")
                }
                MinMaxValue::None => {
                    write!(f, " NO MINVALUE")
                }
                MinMaxValue::Some(minvalue) => {
                    write!(f, " MINVALUE {minvalue}")
                }
            },
            SequenceOptions::MaxValue(value) => match value {
                MinMaxValue::Empty => {
                    write!(f, "")
                }
                MinMaxValue::None => {
                    write!(f, " NO MAXVALUE")
                }
                MinMaxValue::Some(maxvalue) => {
                    write!(f, " MAXVALUE {maxvalue}")
                }
            },
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

/// A privilege on a database object (table, sequence, etc.).
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Action {
    Connect,
    Create,
    Delete,
    Execute,
    Insert { columns: Option<Vec<Ident>> },
    References { columns: Option<Vec<Ident>> },
    Select { columns: Option<Vec<Ident>> },
    Temporary,
    Trigger,
    Truncate,
    Update { columns: Option<Vec<Ident>> },
    Usage,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Action::Connect => f.write_str("CONNECT")?,
            Action::Create => f.write_str("CREATE")?,
            Action::Delete => f.write_str("DELETE")?,
            Action::Execute => f.write_str("EXECUTE")?,
            Action::Insert { .. } => f.write_str("INSERT")?,
            Action::References { .. } => f.write_str("REFERENCES")?,
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

/// Objects on which privileges are granted in a GRANT statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GrantObjects {
    /// Grant privileges on `ALL SEQUENCES IN SCHEMA <schema_name> [, ...]`
    AllSequencesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL TABLES IN SCHEMA <schema_name> [, ...]`
    AllTablesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on specific schemas
    Schemas(Vec<ObjectName>),
    /// Grant privileges on specific sequences
    Sequences(Vec<ObjectName>),
    /// Grant privileges on specific tables
    Tables(Vec<ObjectName>),
}

impl fmt::Display for GrantObjects {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GrantObjects::Sequences(sequences) => {
                write!(f, "SEQUENCE {}", display_comma_separated(sequences))
            }
            GrantObjects::Schemas(schemas) => {
                write!(f, "SCHEMA {}", display_comma_separated(schemas))
            }
            GrantObjects::Tables(tables) => {
                write!(f, "{}", display_comma_separated(tables))
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
        }
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Assignment {
    pub id: Vec<Ident>,
    pub value: Expr,
}

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", display_separated(&self.id, "."), self.value)
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
pub enum FunctionArg {
    Named { name: Ident, arg: FunctionArgExpr },
    Unnamed(FunctionArgExpr),
}

impl fmt::Display for FunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionArg::Named { name, arg } => write!(f, "{name} => {arg}"),
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

/// A function call
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Function {
    pub name: ObjectName,
    pub args: Vec<FunctionArg>,
    /// e.g. `x > 5` in `COUNT(x) FILTER (WHERE x > 5)`
    pub filter: Option<Box<Expr>>,
    // Snowflake/MSSQL supports diffrent options for null treatment in rank functions
    pub null_treatment: Option<NullTreatment>,
    pub over: Option<WindowType>,
    // aggregate functions may specify eg `COUNT(DISTINCT x)`
    pub distinct: bool,
    // Some functions must be called without trailing parentheses, for example Postgres
    // do it for current_catalog, current_schema, etc. This flags is used for formatting.
    pub special: bool,
    // Required ordering for the function (if empty, there is no requirement).
    pub order_by: Vec<OrderByExpr>,
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AnalyzeFormat {
    TEXT,
    GRAPHVIZ,
    JSON,
}

impl fmt::Display for AnalyzeFormat {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            AnalyzeFormat::TEXT => "TEXT",
            AnalyzeFormat::GRAPHVIZ => "GRAPHVIZ",
            AnalyzeFormat::JSON => "JSON",
        })
    }
}

impl fmt::Display for Function {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.special {
            write!(f, "{}", self.name)?;
        } else {
            let order_by = if !self.order_by.is_empty() {
                " ORDER BY "
            } else {
                ""
            };
            write!(
                f,
                "{}({}{}{order_by}{})",
                self.name,
                if self.distinct { "DISTINCT " } else { "" },
                display_comma_separated(&self.args),
                display_comma_separated(&self.order_by),
            )?;

            if let Some(filter_cond) = &self.filter {
                write!(f, " FILTER (WHERE {filter_cond})")?;
            }

            if let Some(o) = &self.null_treatment {
                write!(f, " {o}")?;
            }

            if let Some(o) = &self.over {
                write!(f, " OVER {o}")?;
            }
        }

        Ok(())
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

/// A `LISTAGG` invocation `LISTAGG( [ DISTINCT ] <expr>[, <separator> ] [ON OVERFLOW <on_overflow>] ) )
/// [ WITHIN GROUP (ORDER BY <within_group1>[, ...] ) ]`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ListAgg {
    pub distinct: bool,
    pub expr: Box<Expr>,
    pub separator: Option<Box<Expr>>,
    pub on_overflow: Option<ListAggOnOverflow>,
    pub within_group: Vec<OrderByExpr>,
}

impl fmt::Display for ListAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LISTAGG({}{}",
            if self.distinct { "DISTINCT " } else { "" },
            self.expr
        )?;
        if let Some(separator) = &self.separator {
            write!(f, ", {separator}")?;
        }
        if let Some(on_overflow) = &self.on_overflow {
            write!(f, "{on_overflow}")?;
        }
        write!(f, ")")?;
        if !self.within_group.is_empty() {
            write!(
                f,
                " WITHIN GROUP (ORDER BY {})",
                display_comma_separated(&self.within_group)
            )?;
        }
        Ok(())
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
        write!(f, " ON OVERFLOW")?;
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

/// An `ARRAY_AGG` invocation `ARRAY_AGG( [ DISTINCT ] <expr> [ORDER BY <expr>] [LIMIT <n>] )`
/// Or `ARRAY_AGG( [ DISTINCT ] <expr> ) [ WITHIN GROUP ( ORDER BY <expr> ) ]`
/// ORDER BY position is defined differently for BigQuery, Postgres and Snowflake.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ArrayAgg {
    pub distinct: bool,
    pub expr: Box<Expr>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<Box<Expr>>,
    pub within_group: bool, // order by is used inside a within group or not
}

impl fmt::Display for ArrayAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ARRAY_AGG({}{}",
            if self.distinct { "DISTINCT " } else { "" },
            self.expr
        )?;
        if !self.within_group {
            if let Some(order_by) = &self.order_by {
                write!(f, " ORDER BY {}", display_comma_separated(order_by))?;
            }
            if let Some(limit) = &self.limit {
                write!(f, " LIMIT {limit}")?;
            }
        }
        write!(f, ")")?;
        if self.within_group {
            if let Some(order_by) = &self.order_by {
                write!(
                    f,
                    " WITHIN GROUP (ORDER BY {})",
                    display_comma_separated(order_by)
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ObjectType {
    Table,
    View,
    Index,
    Schema,
    Role,
    Sequence,
    Stage,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::Index => "INDEX",
            ObjectType::Schema => "SCHEMA",
            ObjectType::Role => "ROLE",
            ObjectType::Sequence => "SEQUENCE",
            ObjectType::Stage => "STAGE",
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
    CLUSTERED {
        columns: Vec<Ident>,
        sorted_by: Vec<ColumnDef>,
        num_buckets: i32,
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
    DELIMITED,
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
    pub storage: Option<HiveIOFormat>,
    pub location: Option<String>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SqlOption {
    pub name: Ident,
    pub value: Value,
}

impl fmt::Display for SqlOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", self.name, self.value)
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
}

impl fmt::Display for TransactionIsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionIsolationLevel::*;
        f.write_str(match self {
            ReadUncommitted => "READ UNCOMMITTED",
            ReadCommitted => "READ COMMITTED",
            RepeatableRead => "REPEATABLE READ",
            Serializable => "SERIALIZABLE",
        })
    }
}

/// SQLite specific syntax
///
/// <https://sqlite.org/lang_transaction.html>
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TransactionModifier {
    Deferred,
    Immediate,
    Exclusive,
}

impl fmt::Display for TransactionModifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionModifier::*;
        f.write_str(match self {
            Deferred => "DEFERRED",
            Immediate => "IMMEDIATE",
            Exclusive => "EXCLUSIVE",
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
}

impl fmt::Display for ShowStatementFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => write!(f, "LIKE '{}'", value::escape_single_quote_string(pattern)),
            ILike(pattern) => write!(f, "ILIKE {}", value::escape_single_quote_string(pattern)),
            Where(expr) => write!(f, "WHERE {expr}"),
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
            Rollback => write!(f, "ROLLBACK"),
            Abort => write!(f, "ABORT"),
            Fail => write!(f, "FAIL"),
            Ignore => write!(f, "IGNORE"),
            Replace => write!(f, "REPLACE"),
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
            Stdin { .. } => write!(f, "STDIN"),
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
/// <https://www.postgresql.org/docs/8.4/sql-copy.html>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum CopyLegacyOption {
    /// BINARY
    Binary,
    /// DELIMITER \[ AS \] 'delimiter_character'
    Delimiter(char),
    /// NULL \[ AS \] 'null_string'
    Null(String),
    /// CSV ...
    Csv(Vec<CopyLegacyCsvOption>),
}

impl fmt::Display for CopyLegacyOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use CopyLegacyOption::*;
        match self {
            Binary => write!(f, "BINARY"),
            Delimiter(char) => write!(f, "DELIMITER '{char}'"),
            Null(string) => write!(f, "NULL '{}'", value::escape_single_quote_string(string)),
            Csv(opts) => write!(f, "CSV {}", display_separated(opts, " ")),
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

///
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum MergeClause {
    MatchedUpdate {
        predicate: Option<Expr>,
        assignments: Vec<Assignment>,
    },
    MatchedDelete(Option<Expr>),
    NotMatched {
        predicate: Option<Expr>,
        columns: Vec<Ident>,
        values: Values,
    },
}

impl fmt::Display for MergeClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use MergeClause::*;
        write!(f, "WHEN")?;
        match self {
            MatchedUpdate {
                predicate,
                assignments,
            } => {
                write!(f, " MATCHED")?;
                if let Some(pred) = predicate {
                    write!(f, " AND {pred}")?;
                }
                write!(
                    f,
                    " THEN UPDATE SET {}",
                    display_comma_separated(assignments)
                )
            }
            MatchedDelete(predicate) => {
                write!(f, " MATCHED")?;
                if let Some(pred) = predicate {
                    write!(f, " AND {pred}")?;
                }
                write!(f, " THEN DELETE")
            }
            NotMatched {
                predicate,
                columns,
                values,
            } => {
                write!(f, " NOT MATCHED")?;
                if let Some(pred) = predicate {
                    write!(f, " AND {pred}")?;
                }
                write!(
                    f,
                    " THEN INSERT ({}) {}",
                    display_comma_separated(columns),
                    values
                )
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

/// Optional context modifier for statements that can be or `LOCAL`, or `SESSION`.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ContextModifier {
    /// No context defined. Each dialect defines the default in this scenario.
    None,
    /// `LOCAL` identifier, usually related to transactional states.
    Local,
    /// `SESSION` identifier
    Session,
}

impl fmt::Display for ContextModifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::None => {
                write!(f, "")
            }
            Self::Local => {
                write!(f, " LOCAL")
            }
            Self::Session => {
                write!(f, " SESSION")
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

/// Function describe in DROP FUNCTION.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropFunctionDesc {
    pub name: ObjectName,
    pub args: Option<Vec<OperateFunctionArg>>,
}

impl fmt::Display for DropFunctionDesc {
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

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FunctionDefinition {
    SingleQuotedDef(String),
    DoubleDollarDef(String),
}

impl fmt::Display for FunctionDefinition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionDefinition::SingleQuotedDef(s) => write!(f, "'{s}'")?,
            FunctionDefinition::DoubleDollarDef(s) => write!(f, "$${s}$$")?,
        }
        Ok(())
    }
}

/// Postgres specific feature.
///
/// See [Postgresdocs](https://www.postgresql.org/docs/15/sql-createfunction.html)
/// for more details
#[derive(Debug, Default, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateFunctionBody {
    /// LANGUAGE lang_name
    pub language: Option<Ident>,
    /// IMMUTABLE | STABLE | VOLATILE
    pub behavior: Option<FunctionBehavior>,
    /// AS 'definition'
    ///
    /// Note that Hive's `AS class_name` is also parsed here.
    pub as_: Option<FunctionDefinition>,
    /// RETURN expression
    pub return_: Option<Expr>,
    /// USING ... (Hive only)
    pub using: Option<CreateFunctionUsing>,
}

impl fmt::Display for CreateFunctionBody {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(language) = &self.language {
            write!(f, " LANGUAGE {language}")?;
        }
        if let Some(behavior) = &self.behavior {
            write!(f, " {behavior}")?;
        }
        if let Some(definition) = &self.as_ {
            write!(f, " AS {definition}")?;
        }
        if let Some(expr) = &self.return_ {
            write!(f, " RETURN {expr}")?;
        }
        if let Some(using) = &self.using {
            write!(f, " {using}")?;
        }
        Ok(())
    }
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
    Table(Query),
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

#[cfg(test)]
mod tests {
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
            value: Box::new(Expr::Value(Value::SingleQuotedString(String::from(
                "123:45.67",
            )))),
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
            value: Box::new(Expr::Value(Value::SingleQuotedString(String::from("5")))),
            leading_field: Some(DateTimeField::Second),
            leading_precision: Some(1),
            last_field: None,
            fractional_seconds_precision: Some(3),
        });
        assert_eq!("INTERVAL '5' SECOND (1, 3)", format!("{interval}"));
    }
}
