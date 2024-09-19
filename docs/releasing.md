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

# Releasing

## Prerequisites
Publishing to crates.io has been automated via GitHub Actions, so you will only
need push access to the [sqlparser-rs GitHub repository](https://github.com/sqlparser-rs/sqlparser-rs)
in order to publish a release.

We use the [`cargo release`](https://github.com/sunng87/cargo-release)
subcommand to ensure correct versioning. Install via:

```
$ cargo install cargo-release
```

## Process

1. **Before releasing** ensure `CHANGELOG.md` is updated appropriately and that
    you have a clean checkout of the `main` branch of the sqlparser repository:
    ```
    $ git fetch && git status
    On branch main
    Your branch is up to date with 'origin/main'.

    nothing to commit, working tree clean
    ```
    * If you have the time, check that the examples in the README are up to date.

2. Using `cargo-release` we can publish a new release like so:

    ```
    $ cargo release minor --push-remote origin
    ```

    After verifying, you can rerun with `--execute` if all looks good.
    You can add `--no-push` to stop before actually publishing the release.

    `cargo release` will then:

    * Bump the minor part of the version in `Cargo.toml` (e.g. `0.7.1-alpha.0`
       -> `0.8.0`. You can use `patch` instead of `minor`, as appropriate).
    * Create a new tag (e.g. `v0.8.0`) locally
    * Push the new tag to the specified remote (`origin` in the above
      example), which will trigger a publishing process to crates.io as part of
      the [corresponding GitHub Action](https://github.com/sqlparser-rs/sqlparser-rs/blob/main/.github/workflows/rust.yml).

      Note that credentials for authoring in this way are securely stored in
      the (GitHub) repo secrets as `CRATE_TOKEN`.

4. Check that the new version of the crate is available on crates.io:
    https://crates.io/crates/sqlparser


## `sqlparser_derive` crate

Currently this crate is manually published via `cargo publish`.

crates.io homepage: https://crates.io/crates/sqlparser_derive

```shell
cd derive
cargo publish
```
