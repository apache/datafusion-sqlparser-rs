// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[cfg(feature = "parser")]
mod sqlparser_bigquery;
#[cfg(feature = "parser")]
mod sqlparser_clickhouse;
#[cfg(feature = "parser")]
mod sqlparser_common;
#[cfg(feature = "parser")]
mod sqlparser_custom_dialect;
#[cfg(feature = "parser")]
mod sqlparser_databricks;
#[cfg(feature = "parser")]
mod sqlparser_duckdb;
#[cfg(feature = "parser")]
mod sqlparser_hive;
#[cfg(feature = "parser")]
mod sqlparser_mssql;
#[cfg(feature = "parser")]
mod sqlparser_mysql;
#[cfg(feature = "parser")]
mod sqlparser_postgres;
#[cfg(feature = "parser")]
mod sqlparser_redshift;
#[cfg(feature = "parser")]
mod sqlparser_regression;
#[cfg(feature = "parser")]
mod sqlparser_snowflake;
#[cfg(feature = "parser")]
mod sqlparser_sqlite;
