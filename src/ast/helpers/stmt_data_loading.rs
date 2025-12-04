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
pub struct StageParamsObject {
    pub url: Option<String>,
    pub encryption: KeyValueOptions,
    pub endpoint: Option<String>,
    pub storage_integration: Option<String>,
    pub credentials: KeyValueOptions,
}

/// This enum enables support for both standard SQL select item expressions
/// and Snowflake-specific ones for data loading.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum StageLoadSelectItemKind {
    SelectItem(SelectItem),
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
pub struct StageLoadSelectItem {
    pub alias: Option<Ident>,
    pub file_col_num: i32,
    pub element: Option<Ident>,
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
            write!(f, ":{}", element)?;
        }
        if let Some(item_as) = &self.item_as {
            write!(f, " AS {}", item_as)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct FileStagingCommand {
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub stage: ObjectName,
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
