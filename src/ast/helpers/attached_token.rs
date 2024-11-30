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

use core::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use core::fmt::{self, Debug, Formatter};
use core::hash::{Hash, Hasher};

use crate::tokenizer::{Token, TokenWithSpan};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

/// A wrapper type for attaching tokens to AST nodes that should be ignored in comparisons and hashing.
/// This should be used when a token is not relevant for semantics, but is still needed for
/// accurate source location tracking.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct AttachedToken(pub TokenWithSpan);

impl AttachedToken {
    pub fn empty() -> Self {
        AttachedToken(TokenWithSpan::wrap(Token::EOF))
    }
}

// Conditional Implementations
impl Debug for AttachedToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// Blanket Implementations
impl PartialEq for AttachedToken {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for AttachedToken {}

impl PartialOrd for AttachedToken {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AttachedToken {
    fn cmp(&self, _: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl Hash for AttachedToken {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        // Do nothing
    }
}

impl From<TokenWithSpan> for AttachedToken {
    fn from(value: TokenWithSpan) -> Self {
        AttachedToken(value)
    }
}
