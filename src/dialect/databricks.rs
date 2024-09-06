use crate::dialect::Dialect;

/// A [`Dialect`] for [Databricks SQL](https://www.databricks.com/)
///
/// See <https://docs.databricks.com/en/sql/language-manual/index.html>.
#[derive(Debug, Default)]
pub struct DatabricksDialect;

impl Dialect for DatabricksDialect {
    // see https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        matches!(ch, '`')
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        matches!(ch, 'a'..='z' | 'A'..='Z' | '_')
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_')
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-groupby.html
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_lambda_functions(&self) -> bool {
        true
    }

    // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select.html#syntax
    fn supports_select_wildcard_except(&self) -> bool {
        true
    }

    fn require_interval_qualifier(&self) -> bool {
        true
    }
}
