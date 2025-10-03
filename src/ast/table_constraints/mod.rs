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

//! SQL Abstract Syntax Tree (AST) types for table constraints

mod check_constraint;
pub use check_constraint::CheckConstraint;
mod foreign_key_constraint;
pub use foreign_key_constraint::ForeignKeyConstraint;
mod full_text_or_spatial_constraint;
pub use full_text_or_spatial_constraint::FullTextOrSpatialConstraint;
mod index_constraint;
pub use index_constraint::IndexConstraint;
mod primary_key_constraint;
pub use primary_key_constraint::PrimaryKeyConstraint;
mod unique_constraint;
pub use unique_constraint::UniqueConstraint;
