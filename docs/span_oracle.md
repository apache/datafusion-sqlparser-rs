<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Span oracle

With `--all-features`, every SQL string a test parses through `TestedDialects` also has its spans checked, once per dialect of the set. A node's correct span covers every token the node consumed, including its own keywords and parentheses, and excludes a statement's trailing `;`.

| Check | Applies to | Class | Meaning |
|---|---|---|---|
| `extent` | top-level statements | `empty` | no span |
| | | `inexact` | differs from the first to last token of a single-statement input |
| | | `invalid` | lies outside the input or is inverted |
| | | `overlap` | starts before the previous statement ends |
| `structure` | every node with a span | `empty` | no span, although the node renders text |
| | | `not-empty` | a span, although the node renders nothing |
| | | `invalid` | lies outside the input or is inverted |
| | | `outside-parent` | not inside the nearest ancestor span |
| `reparse` | `Statement`, `Query`, `Select`, `TableFactor`, `Expr`, `OrderByExpr`, `ValueWithSpan`, `Ident`, `ObjectName` | `inexact` | the source under the span does not parse back to the node |
| `edges` | every other node that renders text | `start`, `end`, `both` | the source under the span starts or ends on a different token than the rendering |

The oracle runs with `--all-features` only, because the baseline stores node renderings and `bigdecimal` changes how numbers render.

## Baseline

`tests/span_baseline.tsv` lists every known failure. Its columns are the input hash, the parser options hash, the dialects, the check, the class, the node as `Type::Variant`, the occurrence of that node and text within the input, and the node's text.

A test fails when a finding is missing from the baseline, and also when a baseline finding no longer occurs. The failure message lists both.

- **A new finding** in code you changed is a regression, so fix the span. A new test for syntax whose spans are already incomplete adds findings, so record them.
- **A finding that no longer occurs** means a span got fixed, so record the baseline again.

## Recording

```sh
rm -rf target/span-oracle
SPAN_ORACLE=record cargo test --all-features
find target/span-oracle -name '*.tsv' -exec cat {} + | LC_ALL=C sort -u > tests/span_baseline.tsv
```

CI runs the same recording and fails if the result differs from the committed file.
