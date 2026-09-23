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

# Fuzzing

`cargo-fuzz` needs the nightly toolchain. Install it with `rustup toolchain install nightly`, then:

```shell
cargo install cargo-fuzz
cd fuzz
cargo +nightly fuzz run fuzz_parse_sql fuzz_seeds -- -max_total_time=600
```

There are two targets. `fuzz_parse_sql` parses the input with every dialect.
`fuzz_parse_roundtrip` additionally re-parses the SQL rendered by `Display` and fails when a
rendered statement no longer parses.

ClusterFuzzLite runs continuous fuzzing. Every pull request fuzzes for 10 minutes in
`code-change` mode, a daily batch job grows the shared corpus stored on the
`clusterfuzzlite` branch, and a daily prune compacts it.

`fuzz_seeds/` is a committed corpus of valid SQL that random mutation would never produce.
The run command above uses it and `build.sh` packages it for ClusterFuzzLite.

Crashes land in `artifacts/<target>/` and replay with `cargo fuzz run <target> <crash-file>`.
