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

//! Key-value options for SQL statements.
//! See [this page](https://docs.snowflake.com/en/sql-reference/commands-data-loading) for more details.

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
use core::fmt;
use core::fmt::Formatter;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{display_comma_separated, display_separated, Value};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// A collection of key-value options.
pub struct KeyValueOptions {
    /// The list of key-value options.
    pub options: Vec<KeyValueOption>,
    /// The delimiter used between options.
    pub delimiter: KeyValueOptionsDelimiter,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// The delimiter used between key-value options.
pub enum KeyValueOptionsDelimiter {
    /// Options are separated by spaces.
    Space,
    /// Options are separated by commas.
    Comma,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// A single key-value option.
pub struct KeyValueOption {
    /// The name of the option.
    pub option_name: String,
    /// The value of the option.
    pub option_value: KeyValueOptionKind,
}

/// An option can have a single value, multiple values or a nested list of values.
///
/// A value can be numeric, boolean, etc. Enum-style values are represented
/// as Value::Placeholder. For example: MFA_METHOD=SMS will be represented as
/// `Value::Placeholder("SMS".to_string)`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
/// The kind of value for a key-value option.
pub enum KeyValueOptionKind {
    /// A single value.
    Single(Value),
    /// Multiple values.
    Multi(Vec<Value>),
    /// A nested list of key-value options.
    KeyValueOptions(Box<KeyValueOptions>),
}

impl fmt::Display for KeyValueOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let sep = match self.delimiter {
            KeyValueOptionsDelimiter::Space => " ",
            KeyValueOptionsDelimiter::Comma => ", ",
        };
        write!(f, "{}", display_separated(&self.options, sep))
    }
}

impl fmt::Display for KeyValueOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.option_value {
            KeyValueOptionKind::Single(value) => {
                write!(f, "{}={value}", self.option_name)?;
            }
            KeyValueOptionKind::Multi(values) => {
                write!(
                    f,
                    "{}=({})",
                    self.option_name,
                    display_comma_separated(values)
                )?;
            }
            KeyValueOptionKind::KeyValueOptions(options) => {
                write!(f, "{}=({options})", self.option_name)?;
            }
        }
        Ok(())
    }
}
