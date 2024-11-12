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

/// A wrapper type that ignores the field when comparing or hashing.
pub struct IgnoreField<T>(pub T);

// Conditional Implementations
impl<T: Debug> Debug for IgnoreField<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: Clone> Clone for IgnoreField<T> {
    fn clone(&self) -> Self {
        IgnoreField(self.0.clone())
    }
}

// Blanket Implementations
impl<T> PartialEq for IgnoreField<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl<T> Eq for IgnoreField<T> {}

impl<T> PartialOrd for IgnoreField<T> {
    fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
        Some(Ordering::Equal)
    }
}

impl<T: Ord> Ord for IgnoreField<T> {
    fn cmp(&self, _: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl<T> Hash for IgnoreField<T> {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        // Do nothing
    }
}

impl<T> From<T> for IgnoreField<T> {
    fn from(value: T) -> Self {
        IgnoreField(value)
    }
}
