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

const DIALECTS = [
  { label: "BigQuery", stems: ["bigquery"], pattern: /\bbig\s?query\b/i },
  { label: "ClickHouse", stems: ["clickhouse"], pattern: /\bclick\s?house\b/i },
  { label: "Databricks", stems: ["databricks"], pattern: /\bdatabricks\b/i },
  { label: "DuckDB", stems: ["duckdb"], pattern: /\bduck\s?db\b/i },
  { label: "Hive", stems: ["hive"], pattern: /\bhive\b/i },
  { label: "MySQL", stems: ["mysql"], pattern: /\b(mysql|maria\s?db)\b/i },
  { label: "Oracle", stems: ["oracle"], pattern: /\boracle\b/i },
  { label: "PostgreSQL", stems: ["postgresql", "postgres"], pattern: /\bpostgres/i },
  { label: "Redshift", stems: ["redshift"], pattern: /\bredshift\b/i },
  { label: "Snowflake", stems: ["snowflake"], pattern: /\bsnowflake\b/i },
  { label: "Spark", stems: ["spark"], pattern: /\bspark\b/i },
  { label: "SQL Server", stems: ["mssql"], pattern: /\b(ms\s?sql|sql server|t-?sql)\b/i },
  { label: "SQLite", stems: ["sqlite"], pattern: /\bsqlite\b/i },
  { label: "Teradata", stems: ["teradata"], pattern: /\bteradata\b/i },
];

const DECLARATION = /^[ \t]*dialects?[ \t]*:[ \t]*(.*?)[ \t]*$/im;
const MARKER = "<!-- dialect-labeler -->";
const TRUSTED_ASSOCIATIONS = ["OWNER", "MEMBER", "COLLABORATOR"];

function matchDialects(text) {
  return DIALECTS.filter((d) => d.pattern.test(text)).map((d) => d.label);
}

// `undefined` when no `Dialects:` line exists, otherwise the labels it names (possibly none).
function declaredDialects(text) {
  const value = DECLARATION.exec(text ?? "")?.[1];
  return value ? matchDialects(value) : undefined;
}

function stripMarkup(text) {
  return text.replace(/<!--[\s\S]*?-->/g, "").replace(/```[\s\S]*?```/g, "");
}

function resolve({ title, body }) {
  body ??= "";
  const labels = new Set(matchDialects(title));
  const declared = declaredDialects(body);
  for (const label of declared ?? []) labels.add(label);
  if (labels.size === 0 && declared === undefined) {
    for (const label of matchDialects(stripMarkup(body))) labels.add(label);
  }
  return { labels: [...labels], answered: labels.size > 0 || declared !== undefined };
}

function touchedDialects(paths) {
  const stems = new Set();
  for (const path of paths) {
    const stem = /^src\/dialect\/(\w+)\.rs$|^tests\/sqlparser_(\w+)\.rs$/.exec(path);
    if (stem) stems.add(stem[1] ?? stem[2]);
  }
  return DIALECTS.filter((d) => d.stems.some((s) => stems.has(s))).map((d) => d.label);
}

function isCodeChange(paths) {
  return paths.some((path) => path.startsWith("src/") || path.startsWith("tests/"));
}

function askComment(touched) {
  const lines = [
    MARKER,
    "No dialect label was applied because the title does not name a SQL dialect.",
    "If this change targets one or more dialects, edit the title to `<Dialect>[, <Dialect>]: <description>`, add a `Dialects: <Dialect>[, <Dialect>]` line to the description, or reply with one. Reply `Dialects: none` if the change is dialect-agnostic.",
  ];
  if (touched.length) lines.push(`Changed files touch ${touched.join(", ")}.`);
  return lines.join("\n\n");
}

function trustedComment(comment, issue) {
  if (comment.user.type === "Bot") return false;
  return comment.user.id === issue.user.id || TRUSTED_ASSOCIATIONS.includes(comment.author_association);
}

async function run({ github, context }) {
  const { owner, repo } = context.repo;
  const { payload } = context;
  const issue = payload.pull_request ?? payload.issue;
  const issue_number = issue.number;
  let labels = [];
  let comment;

  if (context.eventName === "issue_comment") {
    if (!trustedComment(payload.comment, issue)) return;
    labels = declaredDialects(payload.comment.body) ?? [];
  } else {
    const resolved = resolve(issue);
    labels = resolved.labels;
    if (!resolved.answered && payload.pull_request && payload.action === "opened") {
      const files = await github.paginate(github.rest.pulls.listFiles, {
        owner,
        repo,
        pull_number: issue_number,
        per_page: 100,
      });
      const paths = files.map((f) => f.filename);
      if (isCodeChange(paths)) comment = askComment(touchedDialects(paths));
    }
  }

  const existing = new Set(issue.labels.map((l) => l.name));
  const missing = labels.filter((l) => !existing.has(l));
  if (missing.length) {
    await github.rest.issues.addLabels({ owner, repo, issue_number, labels: missing });
  }

  if (comment) {
    const comments = await github.paginate(github.rest.issues.listComments, {
      owner,
      repo,
      issue_number,
      per_page: 100,
    });
    if (comments.some((c) => c.body?.includes(MARKER))) return;
    await github.rest.issues.createComment({ owner, repo, issue_number, body: comment });
  }
}

module.exports = run;
