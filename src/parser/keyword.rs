use crate::parser::*;

impl<'a> Parser<'a> {
    /// Parse a keyword-separated list of 1+ items accepted by `F`
    pub fn parse_keyword_separated<T, F>(
        &mut self,
        keyword: Keyword,
        mut f: F,
    ) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser<'a>) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.parse_keyword(keyword) {
                break;
            }
        }
        Ok(values)
    }
}
