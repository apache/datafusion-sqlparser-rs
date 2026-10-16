# ClickHouse lexer

`Parsers/` holds the lexer of ClickHouse, `src/Parsers/` in
<https://github.com/ClickHouse/ClickHouse>, as redistributed by
<https://github.com/ClickHouse/clickhouse-lexer>. `COMMIT` names the ClickHouse
commit the files were taken from. They are © ClickHouse, Inc. and licensed under
the Apache License 2.0, see `LICENSE`.

`fuzz_clickhouse_lexes` compiles them natively with `LEXER_STANDALONE_BUILD`, the
same configuration ClickHouse uses for its WebAssembly build, and calls the C ABI
declared in `Parsers/clickhouse_lexer.h`.

To update, copy `upstream/` from clickhouse-lexer over `Parsers/` and `COMMIT`, and
re-add the provenance header at the top of each file.
