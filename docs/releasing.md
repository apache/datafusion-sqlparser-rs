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
    Your branch is up to date with 'upstream/main'.

    nothing to commit, working tree clean
    ```
    * If you have the time, check that the examples in the README are up to date.

2. Using `cargo-release` we can publish a new release like so:

    ```
    $ cargo release minor --push-remote upstream
    ```

    After verifying, you can rerun with `--execute` if all looks good.
    You can add `--no-push` to stop before actually publishing the release.

    `cargo release` will then:

    * Bump the minor part of the version in `Cargo.toml` (e.g. `0.7.1-alpha.0`
       -> `0.8.0`. You can use `patch` instead of `minor`, as appropriate).
    * Create a new tag (e.g. `v0.8.0`) locally
    * Push the new tag to the specified remote (`upstream` in the above
      example), which will trigger a publishing process to crates.io as part of
      the [corresponding GitHub Action](https://github.com/sqlparser-rs/sqlparser-rs/blob/main/.github/workflows/rust.yml).

      Note that credentials for authoring in this way are securely stored in
      the (GitHub) repo secrets as `CRATE_TOKEN`.
    * Bump the crate version again (to something like `0.8.1-alpha.0`) to
      indicate the start of new development cycle.

3. Push the updates to the `main` branch upstream:
    ```
    $ git push upstream
    ```

4. Check that the new version of the crate is available on crates.io:
    https://crates.io/crates/sqlparser


## `sqlparser_derive` crate

Currently this crate is manually published via `cargo publish`.

crates.io homepage: https://crates.io/crates/sqlparser_derive

```shell
cd derive
cargo publish
```
