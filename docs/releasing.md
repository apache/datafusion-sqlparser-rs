# Releasing

Releasing, i.e. crate publishing, has been automated via GitHub Actions.

In order to author a new release, you simply tag the desired revision and push
the resulting tag.

**Before releasing** ensure `CHANGELOG.md` is updated appropriately as well as
`Cargo.toml`.

## Process

Please ensure you follow the correct format when creating new tags. For
instance:

```
git tag -a '0.6.0' -m '(cargo-release) sqlparser version 0.6.0'
```

This will create a new tag, `0.6.0` which the message,
`(cargo-release) sqlparser version 0.6.0`.

Once the tag is created, pushing the tag upstream will trigger a publishing
process to crates.io. Now to push our example tag:

```
git push origin 0.6.0
```

(Note that this process is fully automated; credentials
for authoring in this way are securely stored in the repo secrets as
`CRATE_TOKEN`.)
