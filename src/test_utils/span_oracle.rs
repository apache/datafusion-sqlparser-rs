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

//! Checks the span of every node in every input the test suite parses,
//! against the known failures in `tests/span_baseline.tsv`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::io::Write as _;
use std::path::Path;
use std::sync::{LazyLock, Mutex};

use crate::ast::{Spanned, Statement};
use crate::dialect::Dialect;
use crate::parser::{Parser, ParserOptions};
use crate::tokenizer::{Location, Span, Token, TokenWithSpan, Tokenizer};

const BASELINE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/span_baseline.tsv");
const RECORD_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/target/span-oracle");

/// Checks the spans of `sql` as parsed by each of `dialects` against the baseline,
/// or appends its findings to the record directory when `SPAN_ORACLE=record`.
pub(super) fn check(
    dialects: &[Box<dyn Dialect>],
    options: Option<&ParserOptions>,
    recursion_limit: Option<usize>,
    sql: &str,
) {
    let recording = std::env::var_os("SPAN_ORACLE").is_some_and(|mode| mode == "record");
    if !recording && BASELINE_FINDINGS.is_none() {
        return;
    }
    let input = format!("{:016x}", fnv64(sql));
    let mut grouped: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    let mut mismatches = String::new();
    for dialect in dialects {
        let options = effective_options(&**dialect, options);
        let Some(found) = findings(&**dialect, &options, recursion_limit, sql) else {
            continue;
        };
        let options = format!("{:016x}", fnv64(&format!("{options:?}")));
        let name = dialect_name(&**dialect);
        let actual: BTreeSet<String> = found.iter().map(ToString::to_string).collect();
        // Recording must not read the baseline, which may hold merge conflict markers.
        if !recording {
            let key = format!("{input}\t{options}\t{name}");
            let expected = BASELINE_FINDINGS
                .as_ref()
                .and_then(|baseline| baseline.get(&key))
                .cloned()
                .unwrap_or_default();
            if let Some(mismatch) = mismatch(&expected, &actual) {
                mismatches.push_str(&format!("\n{name}:{mismatch}"));
            }
        }
        for finding in actual {
            grouped
                .entry((options.clone(), finding))
                .or_default()
                .push(name.clone());
        }
    }

    if recording {
        record(grouped.into_iter().map(|((options, finding), names)| {
            format!("{input}\t{options}\t{}\t{finding}", names.join(","))
        }));
    } else if !mismatches.is_empty() {
        panic!("span oracle mismatch parsing {sql:?}\n{mismatches}\nSee docs/span_oracle.md.");
    }
}

/// The dialect's `Debug` rendering, safe to list with `,` in a tab-separated line.
fn dialect_name(dialect: &dyn Dialect) -> String {
    format!("{dialect:?}").replace(|c: char| c == ',' || c.is_whitespace(), "_")
}

/// The options [`Parser::new`] would use when a test sets none.
fn effective_options(dialect: &dyn Dialect, options: Option<&ParserOptions>) -> ParserOptions {
    options.cloned().unwrap_or_else(|| {
        ParserOptions::new().with_trailing_commas(dialect.supports_trailing_commas())
    })
}

#[derive(Debug, PartialEq, Eq)]
struct Finding {
    check: &'static str,
    class: &'static str,
    node: String,
    occurrence: usize,
    text: String,
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}\t{}\t{}\t{}\t{}",
            self.check,
            self.class,
            escape(&self.node),
            self.occurrence,
            escape(&self.text)
        )
    }
}

/// Counts nodes by label and rendering, so a key survives changes elsewhere in the tree.
#[derive(Default)]
struct Occurrences(HashMap<(String, String), usize>);

impl Occurrences {
    fn next(&mut self, node: &str, text: &str) -> usize {
        let count = self
            .0
            .entry((node.to_string(), text.to_string()))
            .or_default();
        *count += 1;
        *count - 1
    }
}

fn findings(
    dialect: &dyn Dialect,
    options: &ParserOptions,
    recursion_limit: Option<usize>,
    sql: &str,
) -> Option<Vec<Finding>> {
    let mut parser = Parser::new(dialect).with_options(options.clone());
    if let Some(limit) = recursion_limit {
        parser = parser.with_recursion_limit(limit);
    }
    let statements = parser.try_with_sql(sql).ok()?.parse_statements().ok()?;
    let mut found = extent(dialect, options, sql, &statements);
    found.extend(nodes::walk(dialect, options, sql, &statements));
    Some(found)
}

fn extent(
    dialect: &dyn Dialect,
    options: &ParserOptions,
    sql: &str,
    statements: &[Statement],
) -> Vec<Finding> {
    let truth = match statements {
        [_] => token_extent(dialect, options, sql),
        _ => None,
    };
    let mut occurrences = Occurrences::default();
    let mut previous_end = None;
    let mut found = Vec::new();
    for statement in statements {
        let span = statement.span();
        let node = format!("Statement::{}", debug_variant(statement));
        let text = statement.to_string();
        let occurrence = occurrences.next(&node, &text);
        let class = if span == Span::empty() {
            Some("empty")
        } else if slice(sql, span).is_none() {
            Some("invalid")
        } else if let Some(truth) = truth {
            (span != truth).then_some("inexact")
        } else {
            previous_end
                .is_some_and(|end| span.start < end)
                .then_some("overlap")
        };
        if span != Span::empty() {
            previous_end = Some(span.end);
        }
        if let Some(class) = class {
            found.push(Finding {
                check: "extent",
                class,
                node,
                occurrence,
                text,
            });
        }
    }
    found
}

/// First to last significant token, not counting trailing `;`.
fn token_extent(dialect: &dyn Dialect, options: &ParserOptions, sql: &str) -> Option<Span> {
    let tokens = Tokenizer::new(dialect, sql)
        .with_unescape(options.unescape)
        .tokenize_with_location()
        .ok()?;
    let significant = |t: &&TokenWithSpan| !matches!(t.token, Token::Whitespace(_) | Token::EOF);
    let first = tokens.iter().find(significant)?.span.start;
    let last = tokens
        .iter()
        .rev()
        .filter(significant)
        .find(|t| t.token != Token::SemiColon)?
        .span
        .end;
    Some(Span::new(first, last))
}

fn debug_variant(statement: &Statement) -> String {
    format!("{statement:?}")
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// Byte offset of a 1-based line and 1-based `char` column.
fn offset(sql: &str, location: Location) -> Option<usize> {
    let (mut line, mut column) = (1, 1);
    for (index, c) in sql.char_indices() {
        if (line, column) == (location.line, location.column) {
            return Some(index);
        }
        if c == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    ((line, column) == (location.line, location.column)).then_some(sql.len())
}

/// The source text under `span`, if the span lies inside `sql` and is not inverted.
fn slice(sql: &str, span: Span) -> Option<&str> {
    let (start, end) = (offset(sql, span.start)?, offset(sql, span.end)?);
    sql.get(start..end)
}

mod nodes {
    use core::convert::Infallible;
    use core::ops::ControlFlow;

    use super::{slice, Finding, Occurrences};
    use crate::ast::{
        Expr, GroupByExpr, Ident, MergeInsertExpr, MergeUpdateExpr, NodeRef, ObjectName,
        OrderByExpr, Query, Select, Statement, TableFactor, ValueWithSpan, Visit, Visitor,
        WildcardAdditionalOptions,
    };
    use crate::dialect::Dialect;
    use crate::parser::{Parser, ParserError, ParserOptions};
    use crate::tokenizer::{Span, Token, Tokenizer};

    pub(super) fn walk(
        dialect: &dyn Dialect,
        options: &ParserOptions,
        sql: &str,
        statements: &[Statement],
    ) -> Vec<Finding> {
        let mut oracle = Oracle {
            dialect,
            options,
            sql,
            ancestors: Vec::new(),
            occurrences: Occurrences::default(),
            found: Vec::new(),
        };
        for statement in statements {
            match statement.visit(&mut oracle) {
                ControlFlow::Continue(()) => {}
                ControlFlow::Break(never) => match never {},
            }
        }
        oracle.found
    }

    struct Oracle<'a> {
        dialect: &'a dyn Dialect,
        options: &'a ParserOptions,
        sql: &'a str,
        /// The valid span of every node on the path from the statement, `None` where there is none.
        ancestors: Vec<Option<Span>>,
        occurrences: Occurrences,
        found: Vec<Finding>,
    }

    impl Visitor for Oracle<'_> {
        type Break = Infallible;

        fn pre_visit_node(&mut self, node: NodeRef<'_>) -> ControlFlow<Infallible> {
            let span = node
                .spanned()
                .map(|s| s.span())
                .or_else(|| node.downcast_ref::<Ident>().map(|i| i.span));
            let valid = span.filter(|s| *s != Span::empty() && slice(self.sql, *s).is_some());
            // Top-level statements are covered by the extent check.
            if !self.ancestors.is_empty() {
                if let Some(span) = span {
                    self.check(node, span, valid.is_some());
                }
            }
            self.ancestors.push(valid);
            ControlFlow::Continue(())
        }

        fn post_visit_node(&mut self, _node: NodeRef<'_>) -> ControlFlow<Infallible> {
            self.ancestors.pop();
            ControlFlow::Continue(())
        }
    }

    impl Oracle<'_> {
        fn check(&mut self, node: NodeRef<'_>, span: Span, valid: bool) {
            let label = label(node);
            let text = node.display().map(|d| d.to_string());
            let occurrence = self
                .occurrences
                .next(&label, text.as_deref().unwrap_or_default());
            let renders_empty = text.as_deref().map(str::is_empty);
            let parent = self.ancestors.iter().rev().flatten().next();

            let structure = if span == Span::empty() {
                (renders_empty == Some(false) && !stands_for_absent_clause(node)).then_some("empty")
            } else if renders_empty == Some(true) && !renders_in_parent(node) {
                Some("not-empty")
            } else if !valid {
                Some("invalid")
            } else {
                parent
                    .is_some_and(|p| span.start < p.start || span.end > p.end)
                    .then_some("outside-parent")
            };
            // A slice equal to the rendering is exact even where it cannot parse out of context.
            let source = slice(self.sql, span).filter(|_| valid);
            let exact = source.is_some() && source == text.as_deref();
            let reparse = (source.is_some() && !exact && self.reparses(node, span) == Some(false))
                .then_some("inexact");
            let edges = match (source, text.as_deref()) {
                (Some(source), Some(text))
                    if !exact && parse_entry(node).is_none() && !renders_in_parent(node) =>
                {
                    self.edges(source, text)
                }
                _ => None,
            };

            let text = text.unwrap_or_default();
            for (check, class) in [
                ("structure", structure),
                ("reparse", reparse),
                ("edges", edges),
            ] {
                if let Some(class) = class {
                    self.found.push(Finding {
                        check,
                        class,
                        node: label.clone(),
                        occurrence,
                        text: text.clone(),
                    });
                }
            }
        }

        /// Which end of `source` starts or ends on a different token than the rendering.
        fn edges(&self, source: &str, rendered: &str) -> Option<&'static str> {
            let tokens = |sql: &str| -> Option<Vec<Token>> {
                let tokens = Tokenizer::new(self.dialect, sql)
                    .with_unescape(self.options.unescape)
                    .tokenize()
                    .ok()?;
                Some(
                    tokens
                        .into_iter()
                        .filter(|t| !matches!(t, Token::Whitespace(_) | Token::EOF))
                        .collect(),
                )
            };
            let (source, rendered) = (tokens(source)?, tokens(rendered)?);
            let start = same_token(source.first()?, rendered.first()?);
            let end = same_token(source.last()?, rendered.last()?);
            match (start, end) {
                (true, true) => None,
                (false, true) => Some("start"),
                (true, false) => Some("end"),
                (false, false) => Some("both"),
            }
        }

        /// Whether the source under `span` parses back to the node, `None` for types with no parse entry.
        fn reparses(&self, node: NodeRef<'_>, span: Span) -> Option<bool> {
            let parse = parse_entry(node)?;
            let expected = node.display()?.to_string();
            let source = slice(self.sql, span)?;
            let Ok(mut parser) = Parser::new(self.dialect)
                .with_options(self.options.clone())
                .try_with_sql(source)
            else {
                return Some(false);
            };
            Some(
                parse(&mut parser).is_ok_and(|parsed| {
                    parser.peek_token().token == Token::EOF && parsed == expected
                }),
            )
        }
    }

    /// Unquoted words compare case-insensitively, because the rendering uppercases keywords.
    fn same_token(source: &Token, rendered: &Token) -> bool {
        match (source, rendered) {
            (Token::Word(a), Token::Word(b))
                if a.quote_style.is_none() && b.quote_style.is_none() =>
            {
                a.value.eq_ignore_ascii_case(&b.value)
            }
            (Token::Word(a), Token::Word(b)) => {
                (&a.value, a.quote_style) == (&b.value, b.quote_style)
            }
            // Renderings normalize literals, and reparse checks the expressions that hold them.
            _ if is_literal(source) && is_literal(rendered) => {
                core::mem::discriminant(source) == core::mem::discriminant(rendered)
            }
            _ => source == rendered,
        }
    }

    fn is_literal(token: &Token) -> bool {
        matches!(
            token,
            Token::Number(..)
                | Token::SingleQuotedString(_)
                | Token::DoubleQuotedString(_)
                | Token::TripleSingleQuotedString(_)
                | Token::TripleDoubleQuotedString(_)
                | Token::DollarQuotedString(_)
                | Token::SingleQuotedByteStringLiteral(_)
                | Token::DoubleQuotedByteStringLiteral(_)
                | Token::TripleSingleQuotedByteStringLiteral(_)
                | Token::TripleDoubleQuotedByteStringLiteral(_)
                | Token::SingleQuotedRawStringLiteral(_)
                | Token::DoubleQuotedRawStringLiteral(_)
                | Token::TripleSingleQuotedRawStringLiteral(_)
                | Token::TripleDoubleQuotedRawStringLiteral(_)
                | Token::NationalStringLiteral(_)
                | Token::QuoteDelimitedStringLiteral(_)
                | Token::NationalQuoteDelimitedStringLiteral(_)
                | Token::EscapedStringLiteral(_)
                | Token::UnicodeStringLiteral(_)
                | Token::HexStringLiteral(_)
        )
    }

    type ParseEntry = fn(&mut Parser<'_>) -> Result<String, ParserError>;

    fn parse_entry(node: NodeRef<'_>) -> Option<ParseEntry> {
        let entries: [(bool, ParseEntry); 9] = [
            (node.downcast_ref::<Statement>().is_some(), |p| {
                p.parse_statement().map(|v| v.to_string())
            }),
            (node.downcast_ref::<Query>().is_some(), |p| {
                p.parse_query().map(|v| v.to_string())
            }),
            (node.downcast_ref::<Select>().is_some(), |p| {
                p.parse_select().map(|v| v.to_string())
            }),
            (node.downcast_ref::<TableFactor>().is_some(), |p| {
                p.parse_table_factor().map(|v| v.to_string())
            }),
            (node.downcast_ref::<Expr>().is_some(), |p| {
                p.parse_expr().map(|v| v.to_string())
            }),
            (node.downcast_ref::<OrderByExpr>().is_some(), |p| {
                p.parse_order_by_expr().map(|v| v.to_string())
            }),
            (node.downcast_ref::<ValueWithSpan>().is_some(), |p| {
                p.parse_value().map(|v| v.to_string())
            }),
            (node.downcast_ref::<Ident>().is_some(), |p| {
                p.parse_identifier().map(|v| v.to_string())
            }),
            (node.downcast_ref::<ObjectName>().is_some(), |p| {
                p.parse_object_name(false).map(|v| v.to_string())
            }),
        ];
        entries
            .into_iter()
            .find_map(|(matches, entry)| matches.then_some(entry))
    }

    /// Nodes that consumed no tokens yet render text.
    fn stands_for_absent_clause(node: NodeRef<'_>) -> bool {
        node.downcast_ref::<GroupByExpr>().is_some_and(
            |g| matches!(g, GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty()),
        )
    }

    /// Nodes that own a leading token their parent renders, as `*` in [`WildcardAdditionalOptions`].
    fn renders_in_parent(node: NodeRef<'_>) -> bool {
        node.downcast_ref::<WildcardAdditionalOptions>().is_some()
            || node.downcast_ref::<MergeInsertExpr>().is_some()
            || node.downcast_ref::<MergeUpdateExpr>().is_some()
    }

    /// `Type::Variant` with module paths removed, as in `Expr::Cast` or `Parens<Expr>`.
    fn label(node: NodeRef<'_>) -> String {
        let mut label = String::new();
        let mut segment = String::new();
        let mut chars = node.type_name().chars().peekable();
        while let Some(c) = chars.next() {
            if c == ':' && chars.peek() == Some(&':') {
                chars.next();
                segment.clear();
            } else if c.is_alphanumeric() || c == '_' {
                segment.push(c);
            } else {
                label.push_str(&segment);
                segment.clear();
                label.push(c);
            }
        }
        label.push_str(&segment);
        if let Some(variant) = node.variant_name() {
            label.push_str("::");
            label.push_str(variant);
        }
        label
    }
}

/// Baseline findings keyed by input, options and dialect, `None` outside this repository.
static BASELINE_FINDINGS: LazyLock<Option<HashMap<String, BTreeSet<String>>>> =
    LazyLock::new(|| load_baseline(Path::new(BASELINE)));

/// `None` when the file does not exist, as in the published crate, which ships without it.
fn load_baseline(path: &Path) -> Option<HashMap<String, BTreeSet<String>>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => panic!("reading {}: {e}", path.display()),
    };
    let mut findings: HashMap<String, BTreeSet<String>> = HashMap::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.splitn(4, '\t').collect();
        let [input, options, dialects, finding] = fields[..] else {
            panic!("malformed line in {}: {line:?}", path.display());
        };
        for dialect in dialects.split(',') {
            findings
                .entry(format!("{input}\t{options}\t{dialect}"))
                .or_default()
                .insert(finding.to_string());
        }
    }
    Some(findings)
}

fn record(lines: impl Iterator<Item = String>) {
    static WRITE: Mutex<()> = Mutex::new(());
    let lines: Vec<String> = lines.collect();
    if lines.is_empty() {
        return;
    }
    let _guard = WRITE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let path = format!("{RECORD_DIR}/{}.tsv", std::process::id());
    let written = std::fs::create_dir_all(RECORD_DIR).and_then(|()| {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        lines.iter().try_for_each(|line| writeln!(file, "{line}"))
    });
    if let Err(e) = written {
        panic!("recording span findings to {path}: {e}");
    }
}

#[derive(Debug)]
struct Mismatch {
    new: Vec<String>,
    fixed: Vec<String>,
}

fn mismatch(expected: &BTreeSet<String>, actual: &BTreeSet<String>) -> Option<Mismatch> {
    let new: Vec<String> = actual.difference(expected).cloned().collect();
    let fixed: Vec<String> = expected.difference(actual).cloned().collect();
    (!new.is_empty() || !fixed.is_empty()).then_some(Mismatch { new, fixed })
}

impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.new.is_empty() {
            writeln!(f, "\nFindings missing from tests/span_baseline.tsv:")?;
            self.new.iter().try_for_each(|l| writeln!(f, "{l}"))?;
        }
        if !self.fixed.is_empty() {
            writeln!(f, "\nBaseline findings that no longer fail:")?;
            self.fixed.iter().try_for_each(|l| writeln!(f, "{l}"))?;
        }
        Ok(())
    }
}

fn escape(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// FNV-1a, stable across platforms and releases.
fn fnv64(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::GenericDialect;
    use crate::tokenizer::Location;

    fn findings_of(sql: &str) -> Vec<Finding> {
        let dialect = GenericDialect {};
        findings(&dialect, &effective_options(&dialect, None), None, sql).unwrap()
    }

    fn has(findings: &[Finding], check: &str, class: &str, text: &str) -> bool {
        findings
            .iter()
            .any(|f| f.check == check && f.class == class && f.text == text)
    }

    #[test]
    fn offset_counts_chars_from_one() {
        let sql = "é\nab";
        assert_eq!(offset(sql, Location::new(1, 1)), Some(0));
        assert_eq!(offset(sql, Location::new(1, 2)), Some(2));
        assert_eq!(offset(sql, Location::new(2, 2)), Some(4));
        assert_eq!(offset(sql, Location::new(2, 3)), Some(5));
        assert_eq!(offset(sql, Location::new(2, 4)), None);
        assert_eq!(offset(sql, Location::new(0, 0)), None);
    }

    #[test]
    fn correct_spans_produce_no_findings() {
        assert_eq!(findings_of("SELECT a FROM t WHERE b = 1"), []);
    }

    #[test]
    fn truncated_statement_fails_extent() {
        let found = findings_of("SELECT CAST(a AS INT)");
        assert!(has(&found, "extent", "inexact", "SELECT CAST(a AS INT)"));
    }

    #[test]
    fn empty_statement_fails_extent() {
        let found = findings_of("DROP TABLE t");
        assert!(has(&found, "extent", "empty", "DROP TABLE t"));
    }

    #[test]
    fn span_missing_operator_fails_reparse() {
        let found = findings_of("SELECT -x FROM t");
        assert!(has(&found, "reparse", "inexact", "-x"));
    }

    #[test]
    fn child_outside_parent_fails_structure() {
        let found = findings_of("SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t");
        assert!(has(&found, "structure", "outside-parent", "a"));
    }

    #[test]
    fn span_missing_leading_keywords_fails_edges() {
        let found = findings_of("SELECT * FROM a LEFT JOIN b ON a.x = b.x");
        assert!(has(&found, "edges", "start", "LEFT JOIN b ON a.x = b.x"));
    }

    #[test]
    fn edges_ignore_keyword_case() {
        let found = findings_of("select null as x from t");
        assert!(!found
            .iter()
            .any(|f| f.check == "edges" && f.text == "NULL AS x"));
    }

    #[test]
    fn missing_baseline_disables_the_check() {
        let dir = std::env::temp_dir().join(format!("span-oracle-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("missing.tsv");
        let empty = dir.join("empty.tsv");
        std::fs::write(&empty, "").unwrap();

        assert!(load_baseline(&missing).is_none());
        assert_eq!(load_baseline(&empty), Some(HashMap::new()));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn mismatch_reports_both_directions() {
        let lines = |l: &[&str]| l.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>();
        assert!(mismatch(&lines(&["a"]), &lines(&["a"])).is_none());

        let m = mismatch(&lines(&["a"]), &lines(&["a", "b"])).unwrap();
        assert_eq!((m.new, m.fixed), (vec!["b".to_string()], vec![]));

        let m = mismatch(&lines(&["a", "b"]), &lines(&["a"])).unwrap();
        assert_eq!((m.new, m.fixed), (vec![], vec!["b".to_string()]));
    }
}
