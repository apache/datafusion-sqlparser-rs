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

## Installing `honggfuzz`

```
cargo install honggfuzz
```

Install [dependencies](https://github.com/rust-fuzz/honggfuzz-rs#dependencies) for your system.

## Running the fuzzer

Running the fuzzer is as easy as running in the `fuzz` directory.

Choose a target:

These are `[[bin]]` entries in `Cargo.toml`.
List them with `cargo read-manifest | jq '.targets[].name'` from the `fuzz` directory.

Run the fuzzer:

```shell
cd fuzz
cargo hfuzz run <target>
```

After a panic is found, get a stack trace with:

```shell
cargo hfuzz run-debug <target> hfuzz_workspace/<target>/*.fuzz
```

For example, with the `fuzz_parse_sql` target:

```shell
cargo hfuzz run fuzz_parse_sql
cargo hfuzz run-debug fuzz_parse_sql hfuzz_workspace/fuzz_parse_sql/*.fuzz
```
