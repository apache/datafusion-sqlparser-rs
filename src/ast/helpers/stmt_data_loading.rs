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

//! AST types specific to loading and unloading syntax, like one available in Snowflake which
//! contains: STAGE ddl operations, PUT upload or COPY INTO
//! See [this page](https://docs.snowflake.com/en/sql-reference/commands-data-loading) for more details.

#[cfg(not(feature = "std"))]
use alloc::string::String;
use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::ast::helpers::key_value_options::KeyValueOptions;
use crate::ast::{Ident, ObjectName, SelectItem};
#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// Parameters for a named stage object used in data loading/unloading.
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct StageParamsObject {
    /// Optional URL for the stage.
    pub url: Option<String>,
    /// Encryption-related key/value options.
    pub encryption: KeyValueOptions,
    /// Optional endpoint string.
    pub endpoint: Option<String>,
    /// Optional storage integration identifier.
    pub storage_integration: Option<String>,
    /// Credentials for accessing the stage.
    pub credentials: KeyValueOptions,
}

/// This enum enables support for both standard SQL select item expressions
/// and Snowflake-specific ones for data loading.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum StageLoadSelectItemKind {
    /// A standard SQL select item expression.
    SelectItem(SelectItem),
    /// A Snowflake-specific select item used for stage loading.
    StageLoadSelectItem(StageLoadSelectItem),
}

impl fmt::Display for StageLoadSelectItemKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            StageLoadSelectItemKind::SelectItem(item) => write!(f, "{item}"),
            StageLoadSelectItemKind::StageLoadSelectItem(item) => write!(f, "{item}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// A single item in the `SELECT` list for data loading from staged files.
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct StageLoadSelectItem {
    /// Optional alias for the input source.
    pub alias: Option<Ident>,
    /// Column number within the staged file (1-based).
    pub file_col_num: i32,
    /// Optional element identifier following the column reference.
    pub element: Option<Ident>,
    /// Optional alias for the item (AS clause).
    pub item_as: Option<Ident>,
}

impl fmt::Display for StageParamsObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let url = &self.url.as_ref();
        let storage_integration = &self.storage_integration.as_ref();
        let endpoint = &self.endpoint.as_ref();

        if url.is_some() {
            write!(f, " URL='{}'", url.unwrap())?;
        }
        if storage_integration.is_some() {
            write!(f, " STORAGE_INTEGRATION={}", storage_integration.unwrap())?;
        }
        if endpoint.is_some() {
            write!(f, " ENDPOINT='{}'", endpoint.unwrap())?;
        }
        if !self.credentials.options.is_empty() {
            write!(f, " CREDENTIALS=({})", self.credentials)?;
        }
        if !self.encryption.options.is_empty() {
            write!(f, " ENCRYPTION=({})", self.encryption)?;
        }

        Ok(())
    }
}

impl fmt::Display for StageLoadSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(alias) = &self.alias {
            write!(f, "{alias}.")?;
        }
        write!(f, "${}", self.file_col_num)?;
        if let Some(element) = &self.element {
            write!(f, ":{element}")?;
        }
        if let Some(item_as) = &self.item_as {
            write!(f, " AS {item_as}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// A command to stage files to a named stage.
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FileStagingCommand {
    /// The stage to which files are being staged.
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub stage: ObjectName,
    /// Optional file matching `PATTERN` expression.
    pub pattern: Option<String>,
}

impl fmt::Display for FileStagingCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.stage)?;
        if let Some(pattern) = self.pattern.as_ref() {
            write!(f, " PATTERN='{pattern}'")?;
        }
        Ok(())
    }
}
