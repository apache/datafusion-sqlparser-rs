// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// A byte span within the parsed string
#[derive(Debug, Eq, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Span {
    Unset,
    Set { start: usize, end: usize },
}

/// All spans are equal
impl PartialEq for Span {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

/// All spans hash to the same value
impl core::hash::Hash for Span {
    fn hash<H: core::hash::Hasher>(&self, _: &mut H) {}
}

impl Span {
    pub fn new() -> Self {
        Span::Unset
    }

    pub fn expanded(&self, item: &impl Spanned) -> Span {
        match self {
            Span::Unset => item.span(),
            Span::Set { start: s1, end: e1 } => match item.span() {
                Span::Unset => *self,
                Span::Set { start: s2, end: e2 } => {
                    (usize::min(*s1, s2)..usize::max(*e1, e2)).into()
                }
            },
        }
    }

    pub fn expand(&mut self, item: &impl Spanned) {
        *self = self.expanded(item);
    }

    pub fn start(&self) -> Option<usize> {
        match self {
            Span::Unset => None,
            Span::Set { start, .. } => Some(*start),
        }
    }

    pub fn end(&self) -> Option<usize> {
        match self {
            Span::Unset => None,
            Span::Set { end, .. } => Some(*end),
        }
    }

    pub fn range(&self) -> Option<core::ops::Range<usize>> {
        match self {
            Span::Unset => None,
            Span::Set { start, end } => Some(*start..*end),
        }
    }
}

impl Default for Span {
    fn default() -> Self {
        Span::Unset
    }
}

impl core::convert::From<core::ops::Range<usize>> for Span {
    fn from(r: core::ops::Range<usize>) -> Self {
        Self::Set {
            start: r.start,
            end: r.end,
        }
    }
}

pub struct UnsetSpanError;

impl core::convert::TryInto<core::ops::Range<usize>> for Span {
    type Error = UnsetSpanError;

    fn try_into(self) -> Result<core::ops::Range<usize>, Self::Error> {
        match self {
            Span::Unset => Err(UnsetSpanError),
            Span::Set { start, end } => Ok(start..end),
        }
    }
}

pub trait Spanned {
    fn span(&self) -> Span;
}

impl Spanned for Span {
    fn span(&self) -> Span {
        *self
    }
}

impl<T: Spanned> Spanned for Option<T> {
    fn span(&self) -> Span {
        match self {
            Some(v) => v.span(),
            None => Default::default(),
        }
    }
}

impl<T: Spanned> Spanned for Vec<T> {
    fn span(&self) -> Span {
        let mut ans = Span::new();
        for v in self {
            ans.expand(v);
        }
        ans
    }
}

impl<T: Spanned> Spanned for Box<T> {
    fn span(&self) -> Span {
        self.as_ref().span()
    }
}
