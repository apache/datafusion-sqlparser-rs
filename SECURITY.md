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

# Security Policy

This document outlines the security model for `sqlparser-rs` and how to
report vulnerabilities.

## Security Model

`sqlparser-rs` parses SQL text, which is often untrusted input (e.g., a query
string from a user or external system). The parser is expected to reject invalid
or malformed SQL with an error.

Unexpected behavior triggered by malformed or adversarial input is generally
considered a **bug**, not a security vulnerability, unless it is *exploitable**
and could allow an attacker to

* Execute arbitrary code (Remote Code Execution);
* Exfiltrate sensitive information from process memory (Information Disclosure);

For example, panics, crashes, stack overflows, excessive resource consumption,
or infinite loops are generally considered bugs, unless they can be exploited to
achieve one of the above security goals.  If that exploitation path is unclear,
the issue should likely be reported as a bug.

## Reporting a Bug

We treat all bugs seriously and welcome help fixing them. If you find a bug
that does not meet the criteria for a security vulnerability, please report it
in the public issue tracker.

## Reporting a Vulnerability

For security vulnerabilities, **do not file a public issue.** 
Follow the [ASF security reporting process] by emailing [security@apache.org](mailto:security@apache.org).

Include in your report:
- A clear description and minimal reproducer.
- Affected crates and versions.
- A demonstration of the potential impact.

[ASF security reporting process]: https://www.apache.org/security/#reporting-a-vulnerability