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
    DataLoadingOption, DataLoadingOptionType, DataLoadingOptions, StageParamsObject,
};
use crate::ast::Statement;
use crate::dialect::Dialect;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;
#[cfg(not(feature = "std"))]
use alloc::vec;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[derive(Debug, Default)]
pub struct SnowflakeDialect;

impl Dialect for SnowflakeDialect {
    // see https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_ascii_lowercase()
            || ch.is_ascii_uppercase()
            || ch.is_ascii_digit()
            || ch == '$'
            || ch == '_'
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
