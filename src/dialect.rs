use std::fmt;

#[derive(Clone, Debug)]
pub struct Dialect {
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub quote_style: Option<char>
}

impl Default for Dialect {
    fn default() -> Self {
        Self {
            quote_style: None
        }
    }
}

pub trait DialectDisplay {
    fn fmt(&self, f: &mut (dyn fmt::Write), dialect: &Dialect) -> fmt::Result;

    fn sql(&self, dialect: &Dialect) -> Result<String, fmt::Error>
        where
            Self: Sized,
    {
        let mut repr = String::new();
        DialectDisplay::fmt(self, &mut repr, dialect)?;
        Ok(repr)
    }
}
