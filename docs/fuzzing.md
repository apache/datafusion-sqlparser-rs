# Fuzzing

sqlparser uses the tool `cargo-fuzz`.  This tool can be installed using: `cargo install cargo-fuzz`.
Cargo-fuzz also requires nightly to be installed: `rustup install nightly`

Running the fuzzer is as easy as running in the `fuzz` directory:

`cargo +nightly fuzz run parse_sql -- -max_len=64`

