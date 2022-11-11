use core::cmp::Eq;
use core::cmp::Ordering;
use core::hash::{Hash, Hasher};
use std::fmt;

/// Location in input string
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Location {
    /// Line number, starting from 1
    pub line: u64,
    /// Line column, starting from 1
    pub column: u64,
}

impl Location {
    pub fn valid(&self) -> bool {
        self.line > 0 && self.column > 0
    }
}

impl Ord for Location {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.line == other.line {
            self.column.cmp(&other.column)
        } else {
            self.line.cmp(&other.line)
        }
    }
}

impl PartialOrd for Location {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Range {
    pub start: Location,
    pub end: Location,
}

impl Range {
    pub fn valid(&self) -> bool {
        self.start.valid() && self.end.valid()
    }

    pub fn into_option(self) -> Option<Range> {
        if self.valid() {
            Some(self)
        } else {
            None
        }
    }
}

pub struct Located<T, L = Option<Range>> {
    value: T,
    location: L,
}

impl<T, L> Clone for Located<T, L>
where
    T: Clone,
    L: Clone,
{
    fn clone(&self) -> Self {
        Located {
            value: self.value.clone(),
            location: self.location.clone(),
        }
    }
}

impl<T, L> fmt::Debug for Located<T, L>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.value, f)
    }
}

impl<T, L> fmt::Display for Located<T, L>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

impl<T, L> Eq for Located<T, L>
where
    T: Eq,
    L: Eq,
{
}

impl<T, L> Hash for Located<T, L>
where
    T: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<T, L> PartialEq<Self> for Located<T, L>
where
    T: PartialEq,
    L: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value) && self.location.eq(&other.location)
    }
}

impl<T, L> Ord for Located<T, L>
where
    T: Ord,
    L: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.value
            .cmp(&other.value)
            .then_with(|| self.location.cmp(&other.location))
    }
}

impl<T, L> PartialOrd for Located<T, L>
where
    T: PartialOrd,
    L: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.value.partial_cmp(&other.value) {
            Some(Ordering::Equal) => self.location.partial_cmp(&other.location),
            Some(o) => Some(o),
            None => None,
        }
    }
}

impl<T, L> Located<T, L> {
    pub fn new(value: T, location: L) -> Located<T, L> {
        Located { value, location }
    }

    pub fn location(&self) -> &L {
        &self.location
    }

    pub fn get(&self) -> &T {
        &self.value
    }

    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T, L> std::ops::Deref for Located<T, L> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
