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

//! `XMLTABLE`, shared by the dialects that have SQL/XML.

use super::*;

/// True at `XMLTABLE (`.
pub fn at_xml_table<'a, D: Dialect>(p: &Parser<'a, D>) -> bool {
    p.at_keyword("XMLTABLE") && p.peek_nth(1).is_op("(")
}

/// Parses `XMLTABLE(...)`, with `PASSING` required if `require_passing`.
pub fn parse_xml_table<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
    require_passing: bool,
) -> PResult<TableFactor<'a, D::Ext>> {
    p.expect_keyword("XMLTABLE")?;
    p.expect_op("(")?;
    let namespaces = if p.eat_keyword("XMLNAMESPACES") {
        p.expect_op("(")?;
        let namespaces = p.parse_comma_separated(parse_xml_namespace)?;
        p.expect_op(")")?;
        p.expect_op(",")?;
        namespaces
    } else {
        Vec::new()
    };
    let row_expression = p.parse_expr()?;
    let passing = if p.eat_keyword("PASSING") {
        p.parse_comma_separated(parse_xml_passing_argument)?
    } else if require_passing {
        return Err(p.expected("PASSING"));
    } else {
        Vec::new()
    };
    p.expect_keyword("COLUMNS")?;
    let columns = p.parse_comma_separated(parse_xml_table_column)?;
    p.expect_op(")")?;
    Ok(TableFactor::XmlTable(Box::new(XmlTable {
        namespaces,
        row_expression,
        passing,
        columns,
    })))
}

fn parse_xml_namespace<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<XmlNamespace<'a, D::Ext>> {
    let uri = p.parse_expr()?;
    p.expect_keyword("AS")?;
    let name = p.parse_ident()?;
    Ok(XmlNamespace { uri, name })
}

fn parse_xml_passing_argument<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<XmlPassingArgument<'a, D::Ext>> {
    let by_value = p.at_keyword("BY");
    if by_value {
        p.advance();
        p.expect_keyword("VALUE")?;
    }
    let expr = p.parse_expr()?;
    let alias = if p.eat_keyword("AS") {
        Some(p.parse_ident()?)
    } else {
        None
    };
    Ok(XmlPassingArgument {
        by_value,
        expr,
        alias,
    })
}

fn parse_xml_table_column<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<XmlTableColumn<'a, D::Ext>> {
    let name = p.parse_ident()?;
    if p.eat_keyword("FOR") {
        p.expect_keyword("ORDINALITY")?;
        return Ok(XmlTableColumn {
            name,
            kind: XmlColumnKind::ForOrdinality,
        });
    }
    let data_type = Box::new(D::parse_data_type(p)?);
    let (mut path, mut default, mut not_null) = (None, None, false);
    loop {
        if path.is_none() && p.eat_keyword("PATH") {
            path = Some(p.parse_expr()?);
        } else if default.is_none() && p.eat_keyword("DEFAULT") {
            default = Some(p.parse_expr()?);
        } else if !not_null && p.at_keyword("NOT") && p.peek_nth(1).is_word("NULL") {
            p.advance();
            p.advance();
            not_null = true;
        } else if !p.eat_keyword("NULL") {
            break;
        }
    }
    Ok(XmlTableColumn {
        name,
        kind: XmlColumnKind::Data {
            data_type,
            path,
            default,
            not_null,
        },
    })
}
