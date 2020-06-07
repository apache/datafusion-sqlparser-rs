# Benchmarking

Run `cargo bench` in the project `sqlparser_bench` execute the queries.
It will report results using the `criterion` library to perform the benchmarking.

The bench project lives in another crate, to avoid the negative impact on building the `sqlparser` crate.
