# Releasing

Releasing, i.e. crate publishing, has been automated via GitHub Actions.

We use the [`cargo release`](https://github.com/sunng87/cargo-release)
subcommand to ensure correct versioning. Install via:

```
$ cargo install cargo-release
```

**Before releasing** ensure `CHANGELOG.md` is updated appropriately.

## Process

Using `cargo-release` we can author a new minor release like so:

```
$ cargo release minor --skip-publish
```

**Ensure publishing is skipped** since pushing the resulting tag upstream will
handle crate publishing automatically.

This will create a new tag, `0.6.0` with the message,
`(cargo-release) sqlparser version 0.6.0`.

Once the tag is created, pushing the tag upstream will trigger a publishing
process to crates.io. Now to push our example tag:

```
git push origin 0.6.0
```

(Note that this process is fully automated; credentials
for authoring in this way are securely stored in the repo secrets as
`CRATE_TOKEN`.)
