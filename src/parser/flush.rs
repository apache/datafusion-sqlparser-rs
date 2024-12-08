use crate::parser::*;

use crate::parser_err;

impl Parser<'_> {
    pub fn parse_flush(&mut self) -> Result<Statement, ParserError> {
        let mut channel = None;
        let mut tables: Vec<ObjectName> = vec![];
        let mut read_lock = false;
        let mut export = false;

        if !dialect_of!(self is MySqlDialect | GenericDialect) {
            return parser_err!("Unsupported statement FLUSH", self.peek_token().span.start);
        }

        let location = if self.parse_keyword(Keyword::NO_WRITE_TO_BINLOG) {
            Some(FlushLocation::NoWriteToBinlog)
        } else if self.parse_keyword(Keyword::LOCAL) {
            Some(FlushLocation::Local)
        } else {
            None
        };

        let object_type = if self.parse_keywords(&[Keyword::BINARY, Keyword::LOGS]) {
            FlushType::BinaryLogs
        } else if self.parse_keywords(&[Keyword::ENGINE, Keyword::LOGS]) {
            FlushType::EngineLogs
        } else if self.parse_keywords(&[Keyword::ERROR, Keyword::LOGS]) {
            FlushType::ErrorLogs
        } else if self.parse_keywords(&[Keyword::GENERAL, Keyword::LOGS]) {
            FlushType::GeneralLogs
        } else if self.parse_keywords(&[Keyword::HOSTS]) {
            FlushType::Hosts
        } else if self.parse_keyword(Keyword::PRIVILEGES) {
            FlushType::Privileges
        } else if self.parse_keyword(Keyword::OPTIMIZER_COSTS) {
            FlushType::OptimizerCosts
        } else if self.parse_keywords(&[Keyword::RELAY, Keyword::LOGS]) {
            if self.parse_keywords(&[Keyword::FOR, Keyword::CHANNEL]) {
                channel = Some(self.parse_object_name(false).unwrap().to_string());
            }
            FlushType::RelayLogs
        } else if self.parse_keywords(&[Keyword::SLOW, Keyword::LOGS]) {
            FlushType::SlowLogs
        } else if self.parse_keyword(Keyword::STATUS) {
            FlushType::Status
        } else if self.parse_keyword(Keyword::USER_RESOURCES) {
            FlushType::UserResources
        } else if self.parse_keywords(&[Keyword::LOGS]) {
            FlushType::Logs
        } else if self.parse_keywords(&[Keyword::TABLES]) {
            loop {
                let next_token = self.next_token();
                match &next_token.token {
                    Token::Word(w) => match w.keyword {
                        Keyword::WITH => {
                            read_lock = self.parse_keywords(&[Keyword::READ, Keyword::LOCK]);
                        }
                        Keyword::FOR => {
                            export = self.parse_keyword(Keyword::EXPORT);
                        }
                        Keyword::NoKeyword => {
                            self.prev_token();
                            tables = self.parse_comma_separated(|p| p.parse_object_name(false))?;
                        }
                        _ => {}
                    },
                    _ => {
                        break;
                    }
                }
            }

            FlushType::Tables
        } else {
            return self.expected(
                "BINARY LOGS, ENGINE LOGS, ERROR LOGS, GENERAL LOGS, HOSTS, LOGS, PRIVILEGES, OPTIMIZER_COSTS,\
                 RELAY LOGS [FOR CHANNEL channel], SLOW LOGS, STATUS, USER_RESOURCES",
                self.peek_token(),
            );
        };

        Ok(Statement::Flush {
            object_type,
            location,
            channel,
            read_lock,
            export,
            tables,
        })
    }
}
