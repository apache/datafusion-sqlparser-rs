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
use crate::alloc::string::ToString;
use crate::ast::helpers::stmt_data_loading::{
    DataLoadingOption, DataLoadingOptionType, DataLoadingOptions, StageLoadSelectItem,
    StageParamsObject,
};
use crate::ast::{Ident, ObjectName, Statement};
use crate::dialect::Dialect;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;
#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(not(feature = "std"))]
use alloc::{format, vec};

#[derive(Debug, Default)]
pub struct SnowflakeDialect;

impl Dialect for SnowflakeDialect {
    // see https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_' || ch == '@' || ch == '%'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase()
            || ch.is_ascii_uppercase()
            || ch.is_ascii_digit()
            || ch == '$'
            || ch == '_'
            || ch == '/'
            || ch == '~'
    }

    fn supports_within_after_array_aggregation(&self) -> bool {
        true
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if parser.parse_keyword(Keyword::CREATE) {
            // possibly CREATE STAGE
            //[ OR  REPLACE ]
            let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
            //[ TEMPORARY ]
            let temporary = parser.parse_keyword(Keyword::TEMPORARY);

            if parser.parse_keyword(Keyword::STAGE) {
                // OK - this is CREATE STAGE statement
                return Some(parse_create_stage(or_replace, temporary, parser));
            } else {
                // need to go back with the cursor
                let mut back = 1;
                if or_replace {
                    back += 2
                }
                if temporary {
                    back += 1
                }
                for _i in 0..back {
                    parser.prev_token();
                }
            }
        }
        if parser.parse_keywords(&[Keyword::COPY, Keyword::INTO]) {
            // COPY INTO
            return Some(parse_copy_into(parser));
        }

        None
    }
}

pub fn parse_create_stage(
    or_replace: bool,
    temporary: bool,
    parser: &mut Parser,
) -> Result<Statement, ParserError> {
    //[ IF NOT EXISTS ]
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = parser.parse_object_name()?;
    let mut directory_table_params = Vec::new();
    let mut file_format = Vec::new();
    let mut copy_options = Vec::new();
    let mut comment = None;

    // [ internalStageParams | externalStageParams ]
    let stage_params = parse_stage_params(parser)?;

    // [ directoryTableParams ]
    if parser.parse_keyword(Keyword::DIRECTORY) {
        parser.expect_token(&Token::Eq)?;
        directory_table_params = parse_parentheses_options(parser)?;
    }

    // [ file_format]
    if parser.parse_keyword(Keyword::FILE_FORMAT) {
        parser.expect_token(&Token::Eq)?;
        file_format = parse_parentheses_options(parser)?;
    }

    // [ copy_options ]
    if parser.parse_keyword(Keyword::COPY_OPTIONS) {
        parser.expect_token(&Token::Eq)?;
        copy_options = parse_parentheses_options(parser)?;
    }

    // [ comment ]
    if parser.parse_keyword(Keyword::COMMENT) {
        parser.expect_token(&Token::Eq)?;
        comment = Some(match parser.next_token().token {
            Token::SingleQuotedString(word) => Ok(word),
            _ => parser.expected("a comment statement", parser.peek_token()),
        }?)
    }

    Ok(Statement::CreateStage {
        or_replace,
        temporary,
        if_not_exists,
        name,
        stage_params,
        directory_table_params: DataLoadingOptions {
            options: directory_table_params,
        },
        file_format: DataLoadingOptions {
            options: file_format,
        },
        copy_options: DataLoadingOptions {
            options: copy_options,
        },
        comment,
    })
}

pub fn parse_copy_into(parser: &mut Parser) -> Result<Statement, ParserError> {
    let into: ObjectName = parser.parse_object_name()?;
    let mut files: Vec<String> = vec![];
    let mut from_transformations: Option<Vec<StageLoadSelectItem>> = None;
    let from_stage_alias;
    let from_stage: ObjectName;
    let stage_params: StageParamsObject;

    parser.expect_keyword(Keyword::FROM)?;
    // check if data load transformations are present
    match parser.next_token().token {
        Token::LParen => {
            // data load with transformations
            parser.expect_keyword(Keyword::SELECT)?;
            from_transformations = parse_select_items_for_data_load(parser)?;

            parser.expect_keyword(Keyword::FROM)?;
            from_stage = parser.parse_object_name()?;
            stage_params = parse_stage_params(parser)?;

            // as
            from_stage_alias = if parser.parse_keyword(Keyword::AS) {
                Some(match parser.next_token().token {
                    Token::Word(w) => Ok(Ident::new(w.value)),
                    _ => parser.expected("stage alias", parser.peek_token()),
                }?)
            } else {
                None
            };
            parser.expect_token(&Token::RParen)?;
        }
        _ => {
            parser.prev_token();
            from_stage = parser.parse_object_name()?;
            stage_params = parse_stage_params(parser)?;

            // as
            from_stage_alias = if parser.parse_keyword(Keyword::AS) {
                Some(match parser.next_token().token {
                    Token::Word(w) => Ok(Ident::new(w.value)),
                    _ => parser.expected("stage alias", parser.peek_token()),
                }?)
            } else {
                None
            };
        }
    };

    // [ files ]
    if parser.parse_keyword(Keyword::FILES) {
        parser.expect_token(&Token::Eq)?;
        parser.expect_token(&Token::LParen)?;
        let mut continue_loop = true;
        while continue_loop {
            continue_loop = false;
            let next_token = parser.next_token();
            match next_token.token {
                Token::SingleQuotedString(s) => files.push(s),
                _ => parser.expected("file token", next_token)?,
            };
            if parser.next_token().token.eq(&Token::Comma) {
                continue_loop = true;
            } else {
                parser.prev_token(); // not a comma, need to go back
            }
        }
        parser.expect_token(&Token::RParen)?;
    }

    // [ pattern ]
    let mut pattern = None;
    if parser.parse_keyword(Keyword::PATTERN) {
        parser.expect_token(&Token::Eq)?;
        let next_token = parser.next_token();
        pattern = Some(match next_token.token {
            Token::SingleQuotedString(s) => s,
            _ => parser.expected("pattern", next_token)?,
        });
    }

    // [ file_format]
    let mut file_format = Vec::new();
    if parser.parse_keyword(Keyword::FILE_FORMAT) {
        parser.expect_token(&Token::Eq)?;
        file_format = parse_parentheses_options(parser)?;
    }

    // [ copy_options ]
    let mut copy_options = Vec::new();
    if parser.parse_keyword(Keyword::COPY_OPTIONS) {
        parser.expect_token(&Token::Eq)?;
        copy_options = parse_parentheses_options(parser)?;
    }

    // [ VALIDATION_MODE ]
    let mut validation_mode = None;
    if parser.parse_keyword(Keyword::VALIDATION_MODE) {
        parser.expect_token(&Token::Eq)?;
        validation_mode = Some(parser.next_token().token.to_string());
    }

    Ok(Statement::CopyIntoSnowflake {
        into,
        from_stage,
        from_stage_alias,
        stage_params,
        from_transformations,
        files: if files.is_empty() { None } else { Some(files) },
        pattern,
        file_format: DataLoadingOptions {
            options: file_format,
        },
        copy_options: DataLoadingOptions {
            options: copy_options,
        },
        validation_mode,
    })
}

fn parse_select_items_for_data_load(
    parser: &mut Parser,
) -> Result<Option<Vec<StageLoadSelectItem>>, ParserError> {
    // [<alias>.]$<file_col_num>[.<element>] [ , [<alias>.]$<file_col_num>[.<element>] ... ]
    let mut select_items: Vec<StageLoadSelectItem> = vec![];
    loop {
        let mut alias: Option<Ident> = None;
        let mut file_col_num: i32 = 0;
        let mut element: Option<Ident> = None;
        let mut item_as: Option<Ident> = None;

        let next_token = parser.next_token();
        match next_token.token {
            Token::Placeholder(w) => {
                file_col_num = w.to_string().split_off(1).parse::<i32>().map_err(|e| {
                    ParserError::ParserError(format!("Could not parse '{w}' as i32: {e}"))
                })?;
                Ok(())
            }
            Token::Word(w) => {
                alias = Some(Ident::new(w.value));
                Ok(())
            }
            _ => parser.expected("alias or file_col_num", next_token),
        }?;

        if alias.is_some() {
            parser.expect_token(&Token::Period)?;
            // now we get col_num token
            let col_num_token = parser.next_token();
            match col_num_token.token {
                Token::Placeholder(w) => {
                    file_col_num = w.to_string().split_off(1).parse::<i32>().map_err(|e| {
                        ParserError::ParserError(format!("Could not parse '{w}' as i32: {e}"))
                    })?;
                    Ok(())
                }
                _ => parser.expected("file_col_num", col_num_token),
            }?;
        }

        // try extracting optional element
        match parser.next_token().token {
            Token::Colon => {
                // parse element
                element = Some(Ident::new(match parser.next_token().token {
                    Token::Word(w) => Ok(w.value),
                    _ => parser.expected("file_col_num", parser.peek_token()),
                }?));
            }
            _ => {
                // element not present move back
                parser.prev_token();
            }
        }

        // as
        if parser.parse_keyword(Keyword::AS) {
            item_as = Some(match parser.next_token().token {
                Token::Word(w) => Ok(Ident::new(w.value)),
                _ => parser.expected("column item alias", parser.peek_token()),
            }?);
        }

        select_items.push(StageLoadSelectItem {
            alias,
            file_col_num,
            element,
            item_as,
        });

        match parser.next_token().token {
            Token::Comma => {
                // continue
            }
            _ => {
                parser.prev_token(); // need to move back
                break;
            }
        }
    }
    Ok(Some(select_items))
}

fn parse_stage_params(parser: &mut Parser) -> Result<StageParamsObject, ParserError> {
    let (mut url, mut storage_integration, mut endpoint) = (None, None, None);
    let mut encryption: DataLoadingOptions = DataLoadingOptions { options: vec![] };
    let mut credentials: DataLoadingOptions = DataLoadingOptions { options: vec![] };

    // URL
    if parser.parse_keyword(Keyword::URL) {
        parser.expect_token(&Token::Eq)?;
        url = Some(match parser.next_token().token {
            Token::SingleQuotedString(word) => Ok(word),
            _ => parser.expected("a URL statement", parser.peek_token()),
        }?)
    }

    // STORAGE INTEGRATION
    if parser.parse_keyword(Keyword::STORAGE_INTEGRATION) {
        parser.expect_token(&Token::Eq)?;
        storage_integration = Some(parser.next_token().token.to_string());
    }

    // ENDPOINT
    if parser.parse_keyword(Keyword::ENDPOINT) {
        parser.expect_token(&Token::Eq)?;
        endpoint = Some(match parser.next_token().token {
            Token::SingleQuotedString(word) => Ok(word),
            _ => parser.expected("an endpoint statement", parser.peek_token()),
        }?)
    }

    // CREDENTIALS
    if parser.parse_keyword(Keyword::CREDENTIALS) {
        parser.expect_token(&Token::Eq)?;
        credentials = DataLoadingOptions {
            options: parse_parentheses_options(parser)?,
        };
    }

    // ENCRYPTION
    if parser.parse_keyword(Keyword::ENCRYPTION) {
        parser.expect_token(&Token::Eq)?;
        encryption = DataLoadingOptions {
            options: parse_parentheses_options(parser)?,
        };
    }

    Ok(StageParamsObject {
        url,
        encryption,
        endpoint,
        storage_integration,
        credentials,
    })
}

/// Parses options provided within parentheses like:
/// ( ENABLE = { TRUE | FALSE }
///      [ AUTO_REFRESH = { TRUE | FALSE } ]
///      [ REFRESH_ON_CREATE =  { TRUE | FALSE } ]
///      [ NOTIFICATION_INTEGRATION = '<notification_integration_name>' ] )
///
fn parse_parentheses_options(parser: &mut Parser) -> Result<Vec<DataLoadingOption>, ParserError> {
    let mut options: Vec<DataLoadingOption> = Vec::new();

    parser.expect_token(&Token::LParen)?;
    loop {
        match parser.next_token().token {
            Token::RParen => break,
            Token::Word(key) => {
                parser.expect_token(&Token::Eq)?;
                if parser.parse_keyword(Keyword::TRUE) {
                    options.push(DataLoadingOption {
                        option_name: key.value,
                        option_type: DataLoadingOptionType::BOOLEAN,
                        value: "TRUE".to_string(),
                    });
                    Ok(())
                } else if parser.parse_keyword(Keyword::FALSE) {
                    options.push(DataLoadingOption {
                        option_name: key.value,
                        option_type: DataLoadingOptionType::BOOLEAN,
                        value: "FALSE".to_string(),
                    });
                    Ok(())
                } else {
                    match parser.next_token().token {
                        Token::SingleQuotedString(value) => {
                            options.push(DataLoadingOption {
                                option_name: key.value,
                                option_type: DataLoadingOptionType::STRING,
                                value,
                            });
                            Ok(())
                        }
                        Token::Word(word) => {
                            options.push(DataLoadingOption {
                                option_name: key.value,
                                option_type: DataLoadingOptionType::ENUM,
                                value: word.value,
                            });
                            Ok(())
                        }
                        _ => parser.expected("expected option value", parser.peek_token()),
                    }
                }
            }
            _ => parser.expected("another option or ')'", parser.peek_token()),
        }?;
    }
    Ok(options)
}
