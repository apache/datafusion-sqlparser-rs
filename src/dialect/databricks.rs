use crate::dialect::{Dialect, DialectFlags};

/// A [`Dialect`] for [Databricks SQL](https://www.databricks.com/)
///
/// See <https://docs.databricks.com/en/sql/language-manual/index.html>.
#[derive(Debug)]
pub struct DatabricksDialect(DialectFlags);

impl Default for DatabricksDialect {
    fn default() -> Self {
        Self(DialectFlags {
            supports_filter_during_aggregation: true,
            // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-groupby.html
            supports_group_by_expr: true,
            supports_lambda_functions: true,
            // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select.html#syntax
            supports_select_wildcard_except: true,
            require_interval_qualifier: true,
            ..Default::default()
        })
    }
}

/// see <https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html>
impl Dialect for DatabricksDialect {
    fn flags(&self) -> &DialectFlags {
        &self.0
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        matches!(ch, '`')
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        matches!(ch, 'a'..='z' | 'A'..='Z' | '_')
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_')
    }
}
