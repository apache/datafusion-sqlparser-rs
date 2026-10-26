# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Compile a derived dialect after removing the dependency source trees."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]

with tempfile.TemporaryDirectory(prefix="sqlparser-derive-") as scratch:
    scratch = Path(scratch)
    source = scratch / "source"
    source.mkdir()
    for directory in ("src", "derive", "sqlparser_bench", "tests", "examples"):
        shutil.copytree(ROOT / directory, source / directory)
    for filename in ("Cargo.toml", "Cargo.lock"):
        shutil.copy2(ROOT / filename, source / filename)
    target = scratch / "target"
    result = subprocess.run(
        [os.environ.get("CARGO", "cargo"), "build", "--locked", "-p", "sqlparser",
         "--lib", "--no-default-features", "--features", "derive-dialect",
         "--manifest-path", str(source / "Cargo.toml"),
         "--target-dir", str(target), "--message-format=json"],
        check=True, stdout=subprocess.PIPE, text=True,
    )
    artifacts = {}
    for line in result.stdout.splitlines():
        message = json.loads(line)
        if message.get("reason") == "compiler-artifact":
            name = message["target"]["name"]
            if name in ("sqlparser", "sqlparser_derive"):
                artifacts[name] = next(
                    path for path in message["filenames"]
                    if Path(path).suffix in (".rlib", ".so", ".dylib", ".dll")
                )
    shutil.rmtree(source)
    consumer = scratch / "consumer.rs"
    consumer.write_text(r'''
use sqlparser::dialect::{Dialect, MySqlDialect};

sqlparser::derive_dialect!(RootDialect, MySqlDialect);
sqlparser::dialect::derive_dialect!(ModuleDialect, MySqlDialect,
    overrides = { supports_string_literal_backslash_escape = false });
sqlparser_derive::derive_dialect!(DirectDialect, MySqlDialect,
    preserve_type_id = true, overrides = {
        identifier_quote_style = None,
        is_identifier_start = '_' == ch || ch.is_ascii_alphabetic(),
    });

fn main() {
    assert!(RootDialect::new().supports_string_literal_backslash_escape());
    assert!(!ModuleDialect::new().supports_string_literal_backslash_escape());
    let direct = DirectDialect::new();
    assert!(direct.supports_string_literal_backslash_escape());
    assert_eq!(direct.identifier_quote_style("x"), None);
    assert!(direct.is_identifier_start('_'));
    assert!(!direct.is_identifier_start('1'));
    assert!((&direct as &dyn Dialect).is::<MySqlDialect>());
    assert!(sqlparser::parser::Parser::parse_sql(&direct, "SELECT 1").is_ok());
}
''')
    binary = scratch / ("consumer.exe" if os.name == "nt" else "consumer")
    command = [os.environ.get("RUSTC", "rustc"), "--edition=2021", str(consumer),
               "-L", f"dependency={target / 'debug' / 'deps'}", "-o", str(binary)]
    for name, path in artifacts.items():
        command.extend(["--extern", f"{name}={path}"])
    subprocess.run(command, check=True, cwd=scratch,
                   env={**os.environ, "CARGO_MANIFEST_DIR": str(source)})
    subprocess.run([str(binary)], check=True)
    print("Derived dialects compile and run without dependency sources.")
