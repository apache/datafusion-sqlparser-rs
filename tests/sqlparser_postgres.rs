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

#![warn(clippy::all)]
//! Test SQL syntax specific to PostgreSQL. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

#[macro_use]
mod test_utils;
use test_utils::*;

use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
use sqlparser::parser::ParserError;

#[test]
fn parse_create_sequence() {
    // SimpleLogger::new().init().unwrap();

    let sql1 = "CREATE SEQUENCE  name0";
    pg().one_statement_parses_to(sql1, "CREATE SEQUENCE name0");

    let sql2 = "CREATE SEQUENCE  IF NOT EXISTS  name0";
    pg().one_statement_parses_to(sql2, "CREATE SEQUENCE IF NOT EXISTS name0");

    let sql3 = "CREATE TEMPORARY SEQUENCE  IF NOT EXISTS  name0";
    pg().one_statement_parses_to(sql3, "CREATE TEMPORARY SEQUENCE IF NOT EXISTS name0");

    let sql4 = "CREATE TEMPORARY SEQUENCE  name0";
    pg().one_statement_parses_to(sql4, "CREATE TEMPORARY SEQUENCE name0");

    let sql2 = "CREATE TEMPORARY SEQUENCE IF NOT EXISTS  name1
      AS BIGINT
     INCREMENT BY  1
     MINVALUE 1  MAXVALUE 20
     START WITH 10";
    pg().one_statement_parses_to(
        sql2,
        "CREATE TEMPORARY SEQUENCE IF NOT EXISTS name1 AS BIGINT INCREMENT BY 1 MINVALUE 1 MAXVALUE 20 START WITH 10", );

    let sql3 = "CREATE SEQUENCE IF NOT EXISTS  name2
     AS BIGINT
     INCREMENT  1
     MINVALUE 1  MAXVALUE 20
     START WITH 10 CACHE 2 NO CYCLE";
    pg().one_statement_parses_to(
        sql3,
        "CREATE SEQUENCE IF NOT EXISTS name2 AS BIGINT INCREMENT 1 MINVALUE 1 MAXVALUE 20 START WITH 10 CACHE 2 NO CYCLE",
    );

    let sql4 = "CREATE TEMPORARY SEQUENCE  IF NOT EXISTS  name3
         INCREMENT  1
     NO MINVALUE  MAXVALUE 20 CACHE 2 CYCLE";
    pg().one_statement_parses_to(
        sql4,
        "CREATE TEMPORARY SEQUENCE IF NOT EXISTS name3 INCREMENT 1 NO MINVALUE MAXVALUE 20 CACHE 2 CYCLE",
    );

    let sql5 = "CREATE TEMPORARY SEQUENCE  IF NOT EXISTS  name3
         INCREMENT  1
     NO MINVALUE  MAXVALUE 20 OWNED BY public.table01";
    pg().one_statement_parses_to(
        sql5,
        "CREATE TEMPORARY SEQUENCE IF NOT EXISTS name3 INCREMENT 1 NO MINVALUE MAXVALUE 20 OWNED BY public.table01",
    );

    let sql6 = "CREATE TEMPORARY SEQUENCE  IF NOT EXISTS  name3
         INCREMENT  1
     NO MINVALUE  MAXVALUE 20 OWNED BY NONE";
    pg().one_statement_parses_to(
        sql6,
        "CREATE TEMPORARY SEQUENCE IF NOT EXISTS name3 INCREMENT 1 NO MINVALUE MAXVALUE 20 OWNED BY NONE",
    );
}

#[test]
fn parse_drop_sequence() {
    // SimpleLogger::new().init().unwrap();
    let sql1 = "DROP SEQUENCE IF EXISTS  name0 CASCADE";
    pg().one_statement_parses_to(sql1, "DROP SEQUENCE IF EXISTS name0 CASCADE");
    let sql2 = "DROP SEQUENCE IF EXISTS  name1 RESTRICT";
    pg().one_statement_parses_to(sql2, "DROP SEQUENCE IF EXISTS name1 RESTRICT");
    let sql3 = "DROP SEQUENCE  name2 CASCADE";
    pg().one_statement_parses_to(sql3, "DROP SEQUENCE name2 CASCADE");
    let sql4 = "DROP SEQUENCE  name2";
    pg().one_statement_parses_to(sql4, "DROP SEQUENCE name2");
    let sql5 = "DROP SEQUENCE  name0 CASCADE";
    pg().one_statement_parses_to(sql5, "DROP SEQUENCE name0 CASCADE");
    let sql6 = "DROP SEQUENCE  name1 RESTRICT";
    pg().one_statement_parses_to(sql6, "DROP SEQUENCE name1 RESTRICT");
    let sql7 = "DROP SEQUENCE  name1, name2, name3";
    pg().one_statement_parses_to(sql7, "DROP SEQUENCE name1, name2, name3");
}

#[test]
fn parse_create_table_with_defaults() {
    let sql = "CREATE TABLE public.customer (
            customer_id integer DEFAULT nextval(public.customer_customer_id_seq),
            store_id smallint NOT NULL,
            first_name character varying(45) NOT NULL,
            last_name character varying(45) COLLATE \"es_ES\" NOT NULL,
            email character varying(50),
            address_id smallint NOT NULL,
            activebool boolean DEFAULT true NOT NULL,
            create_date date DEFAULT now()::text NOT NULL,
            last_update timestamp without time zone DEFAULT now() NOT NULL,
            active int NOT NULL
    ) WITH (fillfactor = 20, user_catalog_table = true, autovacuum_vacuum_threshold = 100)";
    match pg_and_generic().one_statement_parses_to(sql, "") {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            with_options,
            if_not_exists: false,
            external: false,
            file_format: None,
            location: None,
            ..
        } => {
            assert_eq!("public.customer", name.to_string());
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: "customer_id".into(),
                        data_type: DataType::Integer(None),
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Default(
                                pg().verified_expr("nextval(public.customer_customer_id_seq)")
                            )
                        }],
                    },
                    ColumnDef {
                        name: "store_id".into(),
                        data_type: DataType::SmallInt(None),
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "first_name".into(),
                        data_type: DataType::CharacterVarying(Some(CharacterLength {
                            length: 45,
                            unit: None
                        })),
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "last_name".into(),
                        data_type: DataType::CharacterVarying(Some(CharacterLength {
                            length: 45,
                            unit: None
                        })),
                        collation: Some(ObjectName(vec![Ident::with_quote('"', "es_ES")])),
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull,
                        }],
                    },
                    ColumnDef {
                        name: "email".into(),
                        data_type: DataType::CharacterVarying(Some(CharacterLength {
                            length: 50,
                            unit: None
                        })),
                        collation: None,
                        options: vec![],
                    },
                    ColumnDef {
                        name: "address_id".into(),
                        data_type: DataType::SmallInt(None),
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }],
                    },
                    ColumnDef {
                        name: "activebool".into(),
                        data_type: DataType::Boolean,
                        collation: None,
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Default(Expr::Value(Value::Boolean(true))),
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            }
                        ],
                    },
                    ColumnDef {
                        name: "create_date".into(),
                        data_type: DataType::Date,
                        collation: None,
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Default(
                                    pg().verified_expr("CAST(now() AS TEXT)")
                                )
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            }
                        ],
                    },
                    ColumnDef {
                        name: "last_update".into(),
                        data_type: DataType::Timestamp(None, TimezoneInfo::WithoutTimeZone),
                        collation: None,
                        options: vec![
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Default(pg().verified_expr("now()")),
                            },
                            ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull,
                            }
                        ],
                    },
                    ColumnDef {
                        name: "active".into(),
                        data_type: DataType::Int(None),
                        collation: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }],
                    },
                ]
            );
            assert!(constraints.is_empty());
            assert_eq!(
                with_options,
                vec![
                    SqlOption {
                        name: "fillfactor".into(),
                        value: number("20")
                    },
                    SqlOption {
                        name: "user_catalog_table".into(),
                        value: Value::Boolean(true)
                    },
                    SqlOption {
                        name: "autovacuum_vacuum_threshold".into(),
                        value: number("100")
                    },
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_from_pg_dump() {
    let sql = "CREATE TABLE public.customer (
            customer_id integer DEFAULT nextval('public.customer_customer_id_seq'::regclass) NOT NULL,
            store_id smallint NOT NULL,
            first_name character varying(45) NOT NULL,
            last_name character varying(45) NOT NULL,
            info text[],
            address_id smallint NOT NULL,
            activebool boolean DEFAULT true NOT NULL,
            create_date date DEFAULT now()::DATE NOT NULL,
            create_date1 date DEFAULT 'now'::TEXT::date NOT NULL,
            last_update timestamp without time zone DEFAULT now(),
            release_year public.year,
            active int
        )";
    pg().one_statement_parses_to(sql, "CREATE TABLE public.customer (\
            customer_id INTEGER DEFAULT nextval(CAST('public.customer_customer_id_seq' AS REGCLASS)) NOT NULL, \
            store_id SMALLINT NOT NULL, \
            first_name CHARACTER VARYING(45) NOT NULL, \
            last_name CHARACTER VARYING(45) NOT NULL, \
            info TEXT[], \
            address_id SMALLINT NOT NULL, \
            activebool BOOLEAN DEFAULT true NOT NULL, \
            create_date DATE DEFAULT CAST(now() AS DATE) NOT NULL, \
            create_date1 DATE DEFAULT CAST(CAST('now' AS TEXT) AS DATE) NOT NULL, \
            last_update TIMESTAMP WITHOUT TIME ZONE DEFAULT now(), \
            release_year public.year, \
            active INT\
        )");
}

#[test]
fn parse_create_table_with_inherit() {
    let sql = "\
               CREATE TABLE bazaar.settings (\
               settings_id UUID PRIMARY KEY DEFAULT uuid_generate_v4() NOT NULL, \
               user_id UUID UNIQUE, \
               value TEXT[], \
               use_metric BOOLEAN DEFAULT true\
               )";
    pg().verified_stmt(sql);
}

#[test]
fn parse_create_table_empty() {
    // Zero-column tables are weird, but supported by at least PostgreSQL.
    // <https://github.com/sqlparser-rs/sqlparser-rs/pull/94>
    let _ = pg_and_generic().verified_stmt("CREATE TABLE t ()");
}

#[test]
fn parse_create_table_constraints_only() {
    // Zero-column tables can also have constraints in PostgreSQL
    let sql = "CREATE TABLE t (CONSTRAINT positive CHECK (2 > 1))";
    let ast = pg_and_generic().verified_stmt(sql);
    match ast {
        Statement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => {
            assert_eq!("t", name.to_string());
            assert!(columns.is_empty());
            assert_eq!(
                only(constraints).to_string(),
                "CONSTRAINT positive CHECK (2 > 1)"
            );
        }
        _ => unreachable!(),
    };
}

#[test]
fn parse_alter_table_constraints_rename() {
    match pg().verified_stmt("ALTER TABLE tab RENAME CONSTRAINT old_name TO new_name") {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::RenameConstraint { old_name, new_name },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!(old_name.to_string(), "old_name");
            assert_eq!(new_name.to_string(), "new_name");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_alter_column() {
    pg().one_statement_parses_to(
        "ALTER TABLE tab ALTER COLUMN is_active TYPE TEXT USING 'text'",
        "ALTER TABLE tab ALTER COLUMN is_active SET DATA TYPE TEXT USING 'text'",
    );

    match pg()
        .verified_stmt("ALTER TABLE tab ALTER COLUMN is_active SET DATA TYPE TEXT USING 'text'")
    {
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::AlterColumn { column_name, op },
        } => {
            assert_eq!("tab", name.to_string());
            assert_eq!("is_active", column_name.to_string());
            let using_expr = Expr::Value(Value::SingleQuotedString("text".to_string()));
            assert_eq!(
                op,
                AlterColumnOperation::SetDataType {
                    data_type: DataType::Text,
                    using: Some(using_expr),
                }
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_if_not_exists() {
    let sql = "CREATE TABLE IF NOT EXISTS uk_cities ()";
    let ast = pg_and_generic().verified_stmt(sql);
    match ast {
        Statement::CreateTable {
            name,
            if_not_exists: true,
            ..
        } => {
            assert_eq!("uk_cities", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_bad_if_not_exists() {
    let res = pg().parse_sql_statements("CREATE TABLE NOT EXISTS uk_cities ()");
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: EXISTS".to_string()),
        res.unwrap_err()
    );

    let res = pg().parse_sql_statements("CREATE TABLE IF EXISTS uk_cities ()");
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: EXISTS".to_string()),
        res.unwrap_err()
    );

    let res = pg().parse_sql_statements("CREATE TABLE IF uk_cities ()");
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: uk_cities".to_string()),
        res.unwrap_err()
    );

    let res = pg().parse_sql_statements("CREATE TABLE IF NOT uk_cities ()");
    assert_eq!(
        ParserError::ParserError("Expected end of statement, found: NOT".to_string()),
        res.unwrap_err()
    );
}

#[test]
fn parse_create_schema_if_not_exists() {
    let sql = "CREATE SCHEMA IF NOT EXISTS schema_name";
    let ast = pg_and_generic().verified_stmt(sql);
    match ast {
        Statement::CreateSchema {
            if_not_exists: true,
            schema_name,
        } => assert_eq!("schema_name", schema_name.to_string()),
        _ => unreachable!(),
    }
}

#[test]
fn parse_drop_schema_if_exists() {
    let sql = "DROP SCHEMA IF EXISTS schema_name";
    let ast = pg().verified_stmt(sql);
    match ast {
        Statement::Drop {
            object_type,
            if_exists: true,
            ..
        } => assert_eq!(object_type, ObjectType::Schema),
        _ => unreachable!(),
    }
}

#[test]
fn parse_copy_from_stdin() {
    let sql = r#"COPY public.actor (actor_id, first_name, last_name, last_update, value) FROM stdin;
1	PENELOPE	GUINESS	2006-02-15 09:34:33 0.11111
2	NICK	WAHLBERG	2006-02-15 09:34:33 0.22222
3	ED	CHASE	2006-02-15 09:34:33 0.312323
4	JENNIFER	DAVIS	2006-02-15 09:34:33 0.3232
5	JOHNNY	LOLLOBRIGIDA	2006-02-15 09:34:33 1.343
6	BETTE	NICHOLSON	2006-02-15 09:34:33 5.0
7	GRACE	MOSTEL	2006-02-15 09:34:33 6.0
8	MATTHEW	JOHANSSON	2006-02-15 09:34:33 7.0
9	JOE	SWANK	2006-02-15 09:34:33 8.0
10	CHRISTIAN	GABLE	2006-02-15 09:34:33 9.1
11	ZERO	CAGE	2006-02-15 09:34:33 10.001
12	KARL	BERRY	2017-11-02 19:15:42.308637+08 11.001
A Fateful Reflection of a Waitress And a Boat who must Discover a Sumo Wrestler in Ancient China
Kwara & Kogi
{"Deleted Scenes","Behind the Scenes"}
'awe':5 'awe-inspir':4 'barbarella':1 'cat':13 'conquer':16 'dog':18 'feminist':10 'inspir':6 'monasteri':21 'must':15 'stori':7 'streetcar':2
PHP	₱ USD $
\N  Some other value
\\."#;
    let ast = pg_and_generic().one_statement_parses_to(sql, "");
    println!("{:#?}", ast);
    //assert_eq!(sql, ast.to_string());
}

#[test]
fn test_copy_from() {
    let stmt = pg().verified_stmt("COPY users FROM 'data.csv'");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["users".into()]),
            columns: vec![],
            to: false,
            target: CopyTarget::File {
                filename: "data.csv".to_string(),
            },
            options: vec![],
            legacy_options: vec![],
            values: vec![],
        }
    );

    let stmt = pg().verified_stmt("COPY users FROM 'data.csv' DELIMITER ','");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["users".into()]),
            columns: vec![],
            to: false,
            target: CopyTarget::File {
                filename: "data.csv".to_string(),
            },
            options: vec![],
            legacy_options: vec![CopyLegacyOption::Delimiter(',')],
            values: vec![],
        }
    );

    let stmt = pg().verified_stmt("COPY users FROM 'data.csv' DELIMITER ',' CSV HEADER");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["users".into()]),
            columns: vec![],
            to: false,
            target: CopyTarget::File {
                filename: "data.csv".to_string(),
            },
            options: vec![],
            legacy_options: vec![
                CopyLegacyOption::Delimiter(','),
                CopyLegacyOption::Csv(vec![CopyLegacyCsvOption::Header,])
            ],
            values: vec![],
        }
    );
}

#[test]
fn test_copy_to() {
    let stmt = pg().verified_stmt("COPY users TO 'data.csv'");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["users".into()]),
            columns: vec![],
            to: true,
            target: CopyTarget::File {
                filename: "data.csv".to_string(),
            },
            options: vec![],
            legacy_options: vec![],
            values: vec![],
        }
    );

    let stmt = pg().verified_stmt("COPY users TO 'data.csv' DELIMITER ','");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["users".into()]),
            columns: vec![],
            to: true,
            target: CopyTarget::File {
                filename: "data.csv".to_string(),
            },
            options: vec![],
            legacy_options: vec![CopyLegacyOption::Delimiter(',')],
            values: vec![],
        }
    );

    let stmt = pg().verified_stmt("COPY users TO 'data.csv' DELIMITER ',' CSV HEADER");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["users".into()]),
            columns: vec![],
            to: true,
            target: CopyTarget::File {
                filename: "data.csv".to_string(),
            },
            options: vec![],
            legacy_options: vec![
                CopyLegacyOption::Delimiter(','),
                CopyLegacyOption::Csv(vec![CopyLegacyCsvOption::Header,])
            ],
            values: vec![],
        }
    )
}

#[test]
fn parse_copy_from() {
    let sql = "COPY table (a, b) FROM 'file.csv' WITH
    (
        FORMAT CSV,
        FREEZE,
        FREEZE TRUE,
        FREEZE FALSE,
        DELIMITER ',',
        NULL '',
        HEADER,
        HEADER TRUE,
        HEADER FALSE,
        QUOTE '\"',
        ESCAPE '\\',
        FORCE_QUOTE (a, b),
        FORCE_NOT_NULL (a),
        FORCE_NULL (b),
        ENCODING 'utf8'
    )";
    assert_eq!(
        pg_and_generic().one_statement_parses_to(sql, ""),
        Statement::Copy {
            table_name: ObjectName(vec!["table".into()]),
            columns: vec!["a".into(), "b".into()],
            to: false,
            target: CopyTarget::File {
                filename: "file.csv".into()
            },
            options: vec![
                CopyOption::Format("CSV".into()),
                CopyOption::Freeze(true),
                CopyOption::Freeze(true),
                CopyOption::Freeze(false),
                CopyOption::Delimiter(','),
                CopyOption::Null("".into()),
                CopyOption::Header(true),
                CopyOption::Header(true),
                CopyOption::Header(false),
                CopyOption::Quote('"'),
                CopyOption::Escape('\\'),
                CopyOption::ForceQuote(vec!["a".into(), "b".into()]),
                CopyOption::ForceNotNull(vec!["a".into()]),
                CopyOption::ForceNull(vec!["b".into()]),
                CopyOption::Encoding("utf8".into()),
            ],
            legacy_options: vec![],
            values: vec![],
        }
    );
}

#[test]
fn parse_copy_to() {
    let stmt = pg().verified_stmt("COPY users TO 'data.csv'");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["users".into()]),
            columns: vec![],
            to: true,
            target: CopyTarget::File {
                filename: "data.csv".to_string(),
            },
            options: vec![],
            legacy_options: vec![],
            values: vec![],
        }
    );

    let stmt = pg().verified_stmt("COPY country TO STDOUT (DELIMITER '|')");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["country".into()]),
            columns: vec![],
            to: true,
            target: CopyTarget::Stdout,
            options: vec![CopyOption::Delimiter('|')],
            legacy_options: vec![],
            values: vec![],
        }
    );

    let stmt =
        pg().verified_stmt("COPY country TO PROGRAM 'gzip > /usr1/proj/bray/sql/country_data.gz'");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["country".into()]),
            columns: vec![],
            to: true,
            target: CopyTarget::Program {
                command: "gzip > /usr1/proj/bray/sql/country_data.gz".into(),
            },
            options: vec![],
            legacy_options: vec![],
            values: vec![],
        }
    );
}

#[test]
fn parse_copy_from_before_v9_0() {
    let stmt = pg().verified_stmt("COPY users FROM 'data.csv' BINARY DELIMITER ',' NULL 'null' CSV HEADER QUOTE '\"' ESCAPE '\\' FORCE NOT NULL column");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["users".into()]),
            columns: vec![],
            to: false,
            target: CopyTarget::File {
                filename: "data.csv".to_string(),
            },
            options: vec![],
            legacy_options: vec![
                CopyLegacyOption::Binary,
                CopyLegacyOption::Delimiter(','),
                CopyLegacyOption::Null("null".into()),
                CopyLegacyOption::Csv(vec![
                    CopyLegacyCsvOption::Header,
                    CopyLegacyCsvOption::Quote('\"'),
                    CopyLegacyCsvOption::Escape('\\'),
                    CopyLegacyCsvOption::ForceNotNull(vec!["column".into()]),
                ]),
            ],
            values: vec![],
        }
    );

    // test 'AS' keyword
    let sql = "COPY users FROM 'data.csv' DELIMITER AS ',' NULL AS 'null' CSV QUOTE AS '\"' ESCAPE AS '\\'";
    assert_eq!(
        pg_and_generic().one_statement_parses_to(sql, ""),
        Statement::Copy {
            table_name: ObjectName(vec!["users".into()]),
            columns: vec![],
            to: false,
            target: CopyTarget::File {
                filename: "data.csv".to_string(),
            },
            options: vec![],
            legacy_options: vec![
                CopyLegacyOption::Delimiter(','),
                CopyLegacyOption::Null("null".into()),
                CopyLegacyOption::Csv(vec![
                    CopyLegacyCsvOption::Quote('\"'),
                    CopyLegacyCsvOption::Escape('\\'),
                ]),
            ],
            values: vec![],
        }
    );
}

#[test]
fn parse_copy_to_before_v9_0() {
    let stmt = pg().verified_stmt("COPY users TO 'data.csv' BINARY DELIMITER ',' NULL 'null' CSV HEADER QUOTE '\"' ESCAPE '\\' FORCE QUOTE column");
    assert_eq!(
        stmt,
        Statement::Copy {
            table_name: ObjectName(vec!["users".into()]),
            columns: vec![],
            to: true,
            target: CopyTarget::File {
                filename: "data.csv".to_string(),
            },
            options: vec![],
            legacy_options: vec![
                CopyLegacyOption::Binary,
                CopyLegacyOption::Delimiter(','),
                CopyLegacyOption::Null("null".into()),
                CopyLegacyOption::Csv(vec![
                    CopyLegacyCsvOption::Header,
                    CopyLegacyCsvOption::Quote('\"'),
                    CopyLegacyCsvOption::Escape('\\'),
                    CopyLegacyCsvOption::ForceQuote(vec!["column".into()]),
                ]),
            ],
            values: vec![],
        }
    )
}

#[test]
fn parse_set() {
    let stmt = pg_and_generic().verified_stmt("SET a = b");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            hivevar: false,
            variable: ObjectName(vec![Ident::new("a")]),
            value: vec![Expr::Identifier(Ident {
                value: "b".into(),
                quote_style: None
            })],
        }
    );

    let stmt = pg_and_generic().verified_stmt("SET a = 'b'");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            hivevar: false,
            variable: ObjectName(vec![Ident::new("a")]),
            value: vec![Expr::Value(Value::SingleQuotedString("b".into()))],
        }
    );

    let stmt = pg_and_generic().verified_stmt("SET a = 0");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            hivevar: false,
            variable: ObjectName(vec![Ident::new("a")]),
            value: vec![Expr::Value(Value::Number(
                #[cfg(not(feature = "bigdecimal"))]
                "0".to_string(),
                #[cfg(feature = "bigdecimal")]
                bigdecimal::BigDecimal::from(0),
                false,
            ))],
        }
    );

    let stmt = pg_and_generic().verified_stmt("SET a = DEFAULT");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            hivevar: false,
            variable: ObjectName(vec![Ident::new("a")]),
            value: vec![Expr::Identifier(Ident {
                value: "DEFAULT".into(),
                quote_style: None
            })],
        }
    );

    let stmt = pg_and_generic().verified_stmt("SET LOCAL a = b");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: true,
            hivevar: false,
            variable: ObjectName(vec![Ident::new("a")]),
            value: vec![Expr::Identifier("b".into())],
        }
    );

    let stmt = pg_and_generic().verified_stmt("SET a.b.c = b");
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            hivevar: false,
            variable: ObjectName(vec![Ident::new("a"), Ident::new("b"), Ident::new("c")]),
            value: vec![Expr::Identifier(Ident {
                value: "b".into(),
                quote_style: None
            })],
        }
    );

    let stmt = pg_and_generic().one_statement_parses_to(
        "SET hive.tez.auto.reducer.parallelism=false",
        "SET hive.tez.auto.reducer.parallelism = false",
    );
    assert_eq!(
        stmt,
        Statement::SetVariable {
            local: false,
            hivevar: false,
            variable: ObjectName(vec![
                Ident::new("hive"),
                Ident::new("tez"),
                Ident::new("auto"),
                Ident::new("reducer"),
                Ident::new("parallelism")
            ]),
            value: vec![Expr::Value(Value::Boolean(false))],
        }
    );

    pg_and_generic().one_statement_parses_to("SET a TO b", "SET a = b");
    pg_and_generic().one_statement_parses_to("SET SESSION a = b", "SET a = b");

    assert_eq!(
        pg_and_generic().parse_sql_statements("SET"),
        Err(ParserError::ParserError(
            "Expected identifier, found: EOF".to_string()
        )),
    );

    assert_eq!(
        pg_and_generic().parse_sql_statements("SET a b"),
        Err(ParserError::ParserError(
            "Expected equals sign or TO, found: b".to_string()
        )),
    );

    assert_eq!(
        pg_and_generic().parse_sql_statements("SET a ="),
        Err(ParserError::ParserError(
            "Expected variable value, found: EOF".to_string()
        )),
    );
}

#[test]
fn parse_set_role() {
    let query = "SET SESSION ROLE NONE";
    let stmt = pg_and_generic().verified_stmt(query);
    assert_eq!(
        stmt,
        Statement::SetRole {
            context_modifier: ContextModifier::Session,
            role_name: None,
        }
    );
    assert_eq!(query, stmt.to_string());

    let query = "SET LOCAL ROLE \"rolename\"";
    let stmt = pg_and_generic().verified_stmt(query);
    assert_eq!(
        stmt,
        Statement::SetRole {
            context_modifier: ContextModifier::Local,
            role_name: Some(Ident {
                value: "rolename".to_string(),
                quote_style: Some('\"'),
            }),
        }
    );
    assert_eq!(query, stmt.to_string());

    let query = "SET ROLE 'rolename'";
    let stmt = pg_and_generic().verified_stmt(query);
    assert_eq!(
        stmt,
        Statement::SetRole {
            context_modifier: ContextModifier::None,
            role_name: Some(Ident {
                value: "rolename".to_string(),
                quote_style: Some('\''),
            }),
        }
    );
    assert_eq!(query, stmt.to_string());
}

#[test]
fn parse_show() {
    let stmt = pg_and_generic().verified_stmt("SHOW a a");
    assert_eq!(
        stmt,
        Statement::ShowVariable {
            variable: vec!["a".into(), "a".into()]
        }
    );

    let stmt = pg_and_generic().verified_stmt("SHOW ALL ALL");
    assert_eq!(
        stmt,
        Statement::ShowVariable {
            variable: vec!["ALL".into(), "ALL".into()]
        }
    )
}

#[test]
fn parse_deallocate() {
    let stmt = pg_and_generic().verified_stmt("DEALLOCATE a");
    assert_eq!(
        stmt,
        Statement::Deallocate {
            name: "a".into(),
            prepare: false,
        }
    );

    let stmt = pg_and_generic().verified_stmt("DEALLOCATE ALL");
    assert_eq!(
        stmt,
        Statement::Deallocate {
            name: "ALL".into(),
            prepare: false,
        }
    );

    let stmt = pg_and_generic().verified_stmt("DEALLOCATE PREPARE a");
    assert_eq!(
        stmt,
        Statement::Deallocate {
            name: "a".into(),
            prepare: true,
        }
    );

    let stmt = pg_and_generic().verified_stmt("DEALLOCATE PREPARE ALL");
    assert_eq!(
        stmt,
        Statement::Deallocate {
            name: "ALL".into(),
            prepare: true,
        }
    );
}

#[test]
fn parse_execute() {
    let stmt = pg_and_generic().verified_stmt("EXECUTE a");
    assert_eq!(
        stmt,
        Statement::Execute {
            name: "a".into(),
            parameters: vec![],
        }
    );

    let stmt = pg_and_generic().verified_stmt("EXECUTE a(1, 't')");
    assert_eq!(
        stmt,
        Statement::Execute {
            name: "a".into(),
            parameters: vec![
                Expr::Value(number("1")),
                Expr::Value(Value::SingleQuotedString("t".to_string()))
            ],
        }
    );
}

#[test]
fn parse_prepare() {
    let stmt =
        pg_and_generic().verified_stmt("PREPARE a AS INSERT INTO customers VALUES (a1, a2, a3)");
    let sub_stmt = match stmt {
        Statement::Prepare {
            name,
            data_types,
            statement,
            ..
        } => {
            assert_eq!(name, "a".into());
            assert!(data_types.is_empty());

            statement
        }
        _ => unreachable!(),
    };
    match sub_stmt.as_ref() {
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => {
            assert_eq!(table_name.to_string(), "customers");
            assert!(columns.is_empty());

            let expected_values = [vec![
                Expr::Identifier("a1".into()),
                Expr::Identifier("a2".into()),
                Expr::Identifier("a3".into()),
            ]];
            match &*source.body {
                SetExpr::Values(Values { rows, .. }) => {
                    assert_eq!(rows.as_slice(), &expected_values)
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    };

    let stmt = pg_and_generic()
        .verified_stmt("PREPARE a (INT, TEXT) AS SELECT * FROM customers WHERE customers.id = a1");
    let sub_stmt = match stmt {
        Statement::Prepare {
            name,
            data_types,
            statement,
            ..
        } => {
            assert_eq!(name, "a".into());
            assert_eq!(data_types, vec![DataType::Int(None), DataType::Text]);

            statement
        }
        _ => unreachable!(),
    };
    assert_eq!(
        sub_stmt,
        Box::new(Statement::Query(Box::new(pg_and_generic().verified_query(
            "SELECT * FROM customers WHERE customers.id = a1"
        ))))
    );
}

#[test]
fn parse_pg_on_conflict() {
    let stmt = pg_and_generic().verified_stmt(
        "INSERT INTO distributors (did, dname) \
        VALUES (5, 'Gizmo Transglobal'), (6, 'Associated Computing, Inc')  \
        ON CONFLICT(did) \
        DO UPDATE SET dname = EXCLUDED.dname",
    );
    match stmt {
        Statement::Insert {
            on:
                Some(OnInsert::OnConflict(OnConflict {
                    conflict_target: Some(ConflictTarget::Columns(cols)),
                    action,
                })),
            ..
        } => {
            assert_eq!(vec![Ident::from("did")], cols);
            assert_eq!(
                OnConflictAction::DoUpdate(DoUpdate {
                    assignments: vec![Assignment {
                        id: vec!["dname".into()],
                        value: Expr::CompoundIdentifier(vec!["EXCLUDED".into(), "dname".into()])
                    },],
                    selection: None
                }),
                action
            );
        }
        _ => unreachable!(),
    };

    let stmt = pg_and_generic().verified_stmt(
        "INSERT INTO distributors (did, dname, area) \
        VALUES (5, 'Gizmo Transglobal', 'Mars'), (6, 'Associated Computing, Inc', 'Venus')  \
        ON CONFLICT(did, area) \
        DO UPDATE SET dname = EXCLUDED.dname, area = EXCLUDED.area",
    );
    match stmt {
        Statement::Insert {
            on:
                Some(OnInsert::OnConflict(OnConflict {
                    conflict_target: Some(ConflictTarget::Columns(cols)),
                    action,
                })),
            ..
        } => {
            assert_eq!(vec![Ident::from("did"), Ident::from("area"),], cols);
            assert_eq!(
                OnConflictAction::DoUpdate(DoUpdate {
                    assignments: vec![
                        Assignment {
                            id: vec!["dname".into()],
                            value: Expr::CompoundIdentifier(vec![
                                "EXCLUDED".into(),
                                "dname".into()
                            ])
                        },
                        Assignment {
                            id: vec!["area".into()],
                            value: Expr::CompoundIdentifier(vec!["EXCLUDED".into(), "area".into()])
                        },
                    ],
                    selection: None
                }),
                action
            );
        }
        _ => unreachable!(),
    };

    let stmt = pg_and_generic().verified_stmt(
        "INSERT INTO distributors (did, dname) \
    VALUES (5, 'Gizmo Transglobal'), (6, 'Associated Computing, Inc')  \
    ON CONFLICT DO NOTHING",
    );
    match stmt {
        Statement::Insert {
            on:
                Some(OnInsert::OnConflict(OnConflict {
                    conflict_target: None,
                    action,
                })),
            ..
        } => {
            assert_eq!(OnConflictAction::DoNothing, action);
        }
        _ => unreachable!(),
    };

    let stmt = pg_and_generic().verified_stmt(
        "INSERT INTO distributors (did, dname, dsize) \
        VALUES (5, 'Gizmo Transglobal', 1000), (6, 'Associated Computing, Inc', 1010)  \
        ON CONFLICT(did) \
        DO UPDATE SET dname = $1 WHERE dsize > $2",
    );
    match stmt {
        Statement::Insert {
            on:
                Some(OnInsert::OnConflict(OnConflict {
                    conflict_target: Some(ConflictTarget::Columns(cols)),
                    action,
                })),
            ..
        } => {
            assert_eq!(vec![Ident::from("did")], cols);
            assert_eq!(
                OnConflictAction::DoUpdate(DoUpdate {
                    assignments: vec![Assignment {
                        id: vec!["dname".into()],
                        value: Expr::Value(Value::Placeholder("$1".to_string()))
                    },],
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {
                            value: "dsize".to_string(),
                            quote_style: None
                        })),
                        op: BinaryOperator::Gt,
                        right: Box::new(Expr::Value(Value::Placeholder("$2".to_string())))
                    })
                }),
                action
            );
        }
        _ => unreachable!(),
    };

    let stmt = pg_and_generic().verified_stmt(
        "INSERT INTO distributors (did, dname, dsize) \
        VALUES (5, 'Gizmo Transglobal', 1000), (6, 'Associated Computing, Inc', 1010)  \
        ON CONFLICT ON CONSTRAINT distributors_did_pkey \
        DO UPDATE SET dname = $1 WHERE dsize > $2",
    );
    match stmt {
        Statement::Insert {
            on:
                Some(OnInsert::OnConflict(OnConflict {
                    conflict_target: Some(ConflictTarget::OnConstraint(cname)),
                    action,
                })),
            ..
        } => {
            assert_eq!(vec![Ident::from("distributors_did_pkey")], cname.0);
            assert_eq!(
                OnConflictAction::DoUpdate(DoUpdate {
                    assignments: vec![Assignment {
                        id: vec!["dname".into()],
                        value: Expr::Value(Value::Placeholder("$1".to_string()))
                    },],
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident {
                            value: "dsize".to_string(),
                            quote_style: None
                        })),
                        op: BinaryOperator::Gt,
                        right: Box::new(Expr::Value(Value::Placeholder("$2".to_string())))
                    })
                }),
                action
            );
        }
        _ => unreachable!(),
    };
}

#[test]
fn parse_pg_returning() {
    let stmt = pg_and_generic().verified_stmt(
        "INSERT INTO distributors (did, dname) VALUES (DEFAULT, 'XYZ Widgets') RETURNING did",
    );
    match stmt {
        Statement::Insert { returning, .. } => {
            assert_eq!(
                Some(vec![SelectItem::UnnamedExpr(Expr::Identifier(
                    "did".into()
                )),]),
                returning
            );
        }
        _ => unreachable!(),
    };

    let stmt = pg_and_generic().verified_stmt(
        "UPDATE weather SET temp_lo = temp_lo + 1, temp_hi = temp_lo + 15, prcp = DEFAULT \
             WHERE city = 'San Francisco' AND date = '2003-07-03' \
             RETURNING temp_lo AS lo, temp_hi AS hi, prcp",
    );
    match stmt {
        Statement::Update { returning, .. } => {
            assert_eq!(
                Some(vec![
                    SelectItem::ExprWithAlias {
                        expr: Expr::Identifier("temp_lo".into()),
                        alias: "lo".into()
                    },
                    SelectItem::ExprWithAlias {
                        expr: Expr::Identifier("temp_hi".into()),
                        alias: "hi".into()
                    },
                    SelectItem::UnnamedExpr(Expr::Identifier("prcp".into())),
                ]),
                returning
            );
        }
        _ => unreachable!(),
    };
    let stmt =
        pg_and_generic().verified_stmt("DELETE FROM tasks WHERE status = 'DONE' RETURNING *");
    match stmt {
        Statement::Delete { returning, .. } => {
            assert_eq!(
                Some(vec![SelectItem::Wildcard(
                    WildcardAdditionalOptions::default()
                ),]),
                returning
            );
        }
        _ => unreachable!(),
    };
}

#[test]
fn parse_pg_bitwise_binary_ops() {
    let bitwise_ops = &[
        // Sharp char cannot be used with Generic Dialect, it conflicts with identifiers
        ("#", BinaryOperator::PGBitwiseXor, pg()),
        (">>", BinaryOperator::PGBitwiseShiftRight, pg_and_generic()),
        ("<<", BinaryOperator::PGBitwiseShiftLeft, pg_and_generic()),
    ];

    for (str_op, op, dialects) in bitwise_ops {
        let select = dialects.verified_only_select(&format!("SELECT a {} b", &str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("a"))),
                op: op.clone(),
                right: Box::new(Expr::Identifier(Ident::new("b"))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn parse_pg_unary_ops() {
    let pg_unary_ops = &[
        ("~", UnaryOperator::PGBitwiseNot),
        ("|/", UnaryOperator::PGSquareRoot),
        ("||/", UnaryOperator::PGCubeRoot),
        ("!!", UnaryOperator::PGPrefixFactorial),
        ("@", UnaryOperator::PGAbs),
    ];

    for (str_op, op) in pg_unary_ops {
        let select = pg().verified_only_select(&format!("SELECT {}a", &str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::UnaryOp {
                op: *op,
                expr: Box::new(Expr::Identifier(Ident::new("a"))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn parse_pg_postfix_factorial() {
    let postfix_factorial = &[("!", UnaryOperator::PGPostfixFactorial)];

    for (str_op, op) in postfix_factorial {
        let select = pg().verified_only_select(&format!("SELECT a{}", &str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::UnaryOp {
                op: *op,
                expr: Box::new(Expr::Identifier(Ident::new("a"))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn parse_pg_regex_match_ops() {
    let pg_regex_match_ops = &[
        ("~", BinaryOperator::PGRegexMatch),
        ("~*", BinaryOperator::PGRegexIMatch),
        ("!~", BinaryOperator::PGRegexNotMatch),
        ("!~*", BinaryOperator::PGRegexNotIMatch),
    ];

    for (str_op, op) in pg_regex_match_ops {
        let select = pg().verified_only_select(&format!("SELECT 'abc' {} '^a'", &str_op));
        assert_eq!(
            SelectItem::UnnamedExpr(Expr::BinaryOp {
                left: Box::new(Expr::Value(Value::SingleQuotedString("abc".into()))),
                op: op.clone(),
                right: Box::new(Expr::Value(Value::SingleQuotedString("^a".into()))),
            }),
            select.projection[0]
        );
    }
}

#[test]
fn parse_array_index_expr() {
    #[cfg(feature = "bigdecimal")]
    let num: Vec<Expr> = (0..=10)
        .into_iter()
        .map(|s| Expr::Value(Value::Number(bigdecimal::BigDecimal::from(s), false)))
        .collect();
    #[cfg(not(feature = "bigdecimal"))]
    let num: Vec<Expr> = (0..=10)
        .into_iter()
        .map(|s| Expr::Value(Value::Number(s.to_string(), false)))
        .collect();

    let sql = "SELECT foo[0] FROM foos";
    let select = pg_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::ArrayIndex {
            obj: Box::new(Expr::Identifier(Ident::new("foo"))),
            indexes: vec![num[0].clone()],
        },
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT foo[0][0] FROM foos";
    let select = pg_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::ArrayIndex {
            obj: Box::new(Expr::Identifier(Ident::new("foo"))),
            indexes: vec![num[0].clone(), num[0].clone()],
        },
        expr_from_projection(only(&select.projection)),
    );

    let sql = r#"SELECT bar[0]["baz"]["fooz"] FROM foos"#;
    let select = pg_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::ArrayIndex {
            obj: Box::new(Expr::Identifier(Ident::new("bar"))),
            indexes: vec![
                num[0].clone(),
                Expr::Identifier(Ident {
                    value: "baz".to_string(),
                    quote_style: Some('"')
                }),
                Expr::Identifier(Ident {
                    value: "fooz".to_string(),
                    quote_style: Some('"')
                })
            ],
        },
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT (CAST(ARRAY[ARRAY[2, 3]] AS INT[][]))[1][2]";
    let select = pg_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::ArrayIndex {
            obj: Box::new(Expr::Nested(Box::new(Expr::Cast {
                expr: Box::new(Expr::Array(Array {
                    elem: vec![Expr::Array(Array {
                        elem: vec![num[2].clone(), num[3].clone(),],
                        named: true,
                    })],
                    named: true,
                })),
                data_type: DataType::Array(Some(Box::new(DataType::Array(Some(Box::new(
                    DataType::Int(None)
                ))))))
            }))),
            indexes: vec![num[1].clone(), num[2].clone()],
        },
        expr_from_projection(only(&select.projection)),
    );

    let sql = "SELECT ARRAY[]";
    let select = pg_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Array(sqlparser::ast::Array {
            elem: vec![],
            named: true
        }),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_array_subquery_expr() {
    let sql = "SELECT ARRAY(SELECT 1 UNION SELECT 2)";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        &Expr::ArraySubquery(Box::new(Query {
            with: None,
            body: Box::new(SetExpr::SetOperation {
                op: SetOperator::Union,
                set_quantifier: SetQuantifier::None,
                left: Box::new(SetExpr::Select(Box::new(Select {
                    distinct: false,
                    top: None,
                    projection: vec![SelectItem::UnnamedExpr(Expr::Value(Value::Number(
                        #[cfg(not(feature = "bigdecimal"))]
                        "1".to_string(),
                        #[cfg(feature = "bigdecimal")]
                        bigdecimal::BigDecimal::from(1),
                        false,
                    )))],
                    into: None,
                    from: vec![],
                    lateral_views: vec![],
                    selection: None,
                    group_by: vec![],
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    qualify: None,
                }))),
                right: Box::new(SetExpr::Select(Box::new(Select {
                    distinct: false,
                    top: None,
                    projection: vec![SelectItem::UnnamedExpr(Expr::Value(Value::Number(
                        #[cfg(not(feature = "bigdecimal"))]
                        "2".to_string(),
                        #[cfg(feature = "bigdecimal")]
                        bigdecimal::BigDecimal::from(2),
                        false,
                    )))],
                    into: None,
                    from: vec![],
                    lateral_views: vec![],
                    selection: None,
                    group_by: vec![],
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    qualify: None,
                }))),
            }),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            locks: vec![],
        })),
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn test_transaction_statement() {
    let statement = pg().verified_stmt("SET TRANSACTION SNAPSHOT '000003A1-1'");
    assert_eq!(
        statement,
        Statement::SetTransaction {
            modes: vec![],
            snapshot: Some(Value::SingleQuotedString(String::from("000003A1-1"))),
            session: false
        }
    );
    let statement = pg().verified_stmt("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE");
    assert_eq!(
        statement,
        Statement::SetTransaction {
            modes: vec![
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly),
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite),
                TransactionMode::IsolationLevel(TransactionIsolationLevel::Serializable),
            ],
            snapshot: None,
            session: true
        }
    );
}

#[test]
fn test_savepoint() {
    match pg().verified_stmt("SAVEPOINT test1") {
        Statement::Savepoint { name } => {
            assert_eq!(Ident::new("test1"), name);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_json() {
    let sql = "SELECT params ->> 'name' FROM events";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            left: Box::new(Expr::Identifier(Ident::new("params"))),
            operator: JsonOperator::LongArrow,
            right: Box::new(Expr::Value(Value::SingleQuotedString("name".to_string()))),
        }),
        select.projection[0]
    );

    let sql = "SELECT params -> 'name' FROM events";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            left: Box::new(Expr::Identifier(Ident::new("params"))),
            operator: JsonOperator::Arrow,
            right: Box::new(Expr::Value(Value::SingleQuotedString("name".to_string()))),
        }),
        select.projection[0]
    );

    let sql = "SELECT info -> 'items' ->> 'product' FROM orders";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            left: Box::new(Expr::Identifier(Ident::new("info"))),
            operator: JsonOperator::Arrow,
            right: Box::new(Expr::JsonAccess {
                left: Box::new(Expr::Value(Value::SingleQuotedString("items".to_string()))),
                operator: JsonOperator::LongArrow,
                right: Box::new(Expr::Value(Value::SingleQuotedString(
                    "product".to_string()
                )))
            }),
        }),
        select.projection[0]
    );

    let sql = "SELECT info #> '{a,b,c}' FROM orders";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            left: Box::new(Expr::Identifier(Ident::new("info"))),
            operator: JsonOperator::HashArrow,
            right: Box::new(Expr::Value(Value::SingleQuotedString(
                "{a,b,c}".to_string()
            ))),
        }),
        select.projection[0]
    );

    let sql = "SELECT info #>> '{a,b,c}' FROM orders";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            left: Box::new(Expr::Identifier(Ident::new("info"))),
            operator: JsonOperator::HashLongArrow,
            right: Box::new(Expr::Value(Value::SingleQuotedString(
                "{a,b,c}".to_string()
            ))),
        }),
        select.projection[0]
    );

    let sql = "SELECT info FROM orders WHERE info @> '{\"a\": 1}'";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        Expr::JsonAccess {
            left: Box::new(Expr::Identifier(Ident::new("info"))),
            operator: JsonOperator::AtArrow,
            right: Box::new(Expr::Value(Value::SingleQuotedString(
                "{\"a\": 1}".to_string()
            ))),
        },
        select.selection.unwrap(),
    );

    let sql = "SELECT info FROM orders WHERE '{\"a\": 1}' <@ info";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        Expr::JsonAccess {
            left: Box::new(Expr::Value(Value::SingleQuotedString(
                "{\"a\": 1}".to_string()
            ))),
            operator: JsonOperator::ArrowAt,
            right: Box::new(Expr::Identifier(Ident::new("info"))),
        },
        select.selection.unwrap(),
    );

    let sql = "SELECT info #- ARRAY['a', 'b'] FROM orders";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::JsonAccess {
            left: Box::new(Expr::Identifier(Ident::from("info"))),
            operator: JsonOperator::HashMinus,
            right: Box::new(Expr::Array(Array {
                elem: vec![
                    Expr::Value(Value::SingleQuotedString("a".to_string())),
                    Expr::Value(Value::SingleQuotedString("b".to_string())),
                ],
                named: true,
            })),
        }),
        select.projection[0],
    );

    let sql = "SELECT info FROM orders WHERE info @? '$.a'";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        Expr::JsonAccess {
            left: Box::new(Expr::Identifier(Ident::from("info"))),
            operator: JsonOperator::AtQuestion,
            right: Box::new(Expr::Value(Value::SingleQuotedString("$.a".to_string())),),
        },
        select.selection.unwrap(),
    );

    let sql = "SELECT info FROM orders WHERE info @@ '$.a'";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        Expr::JsonAccess {
            left: Box::new(Expr::Identifier(Ident::from("info"))),
            operator: JsonOperator::AtAt,
            right: Box::new(Expr::Value(Value::SingleQuotedString("$.a".to_string())),),
        },
        select.selection.unwrap(),
    );
}

#[test]
fn test_composite_value() {
    let sql = "SELECT (on_hand.item).name FROM on_hand WHERE (on_hand.item).price > 9";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::CompositeAccess {
            key: Ident::new("name"),
            expr: Box::new(Expr::Nested(Box::new(Expr::CompoundIdentifier(vec![
                Ident::new("on_hand"),
                Ident::new("item")
            ]))))
        }),
        select.projection[0]
    );

    #[cfg(feature = "bigdecimal")]
    let num: Expr = Expr::Value(Value::Number(bigdecimal::BigDecimal::from(9), false));
    #[cfg(not(feature = "bigdecimal"))]
    let num: Expr = Expr::Value(Value::Number("9".to_string(), false));
    assert_eq!(
        select.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::CompositeAccess {
                key: Ident::new("price"),
                expr: Box::new(Expr::Nested(Box::new(Expr::CompoundIdentifier(vec![
                    Ident::new("on_hand"),
                    Ident::new("item")
                ]))))
            }),
            op: BinaryOperator::Gt,
            right: Box::new(num)
        })
    );

    let sql = "SELECT (information_schema._pg_expandarray(ARRAY['i', 'i'])).n";
    let select = pg().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(Expr::CompositeAccess {
            key: Ident::new("n"),
            expr: Box::new(Expr::Nested(Box::new(Expr::Function(Function {
                name: ObjectName(vec![
                    Ident::new("information_schema"),
                    Ident::new("_pg_expandarray")
                ]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Array(
                    Array {
                        elem: vec![
                            Expr::Value(Value::SingleQuotedString("i".to_string())),
                            Expr::Value(Value::SingleQuotedString("i".to_string())),
                        ],
                        named: true
                    }
                )))],
                over: None,
                distinct: false,
                special: false
            }))))
        }),
        select.projection[0]
    );
}

#[test]
fn parse_comments() {
    match pg().verified_stmt("COMMENT ON COLUMN tab.name IS 'comment'") {
        Statement::Comment {
            object_type,
            object_name,
            comment: Some(comment),
        } => {
            assert_eq!("comment", comment);
            assert_eq!("tab.name", object_name.to_string());
            assert_eq!(CommentObject::Column, object_type);
        }
        _ => unreachable!(),
    }

    match pg().verified_stmt("COMMENT ON TABLE public.tab IS 'comment'") {
        Statement::Comment {
            object_type,
            object_name,
            comment: Some(comment),
        } => {
            assert_eq!("comment", comment);
            assert_eq!("public.tab", object_name.to_string());
            assert_eq!(CommentObject::Table, object_type);
        }
        _ => unreachable!(),
    }

    match pg().verified_stmt("COMMENT ON TABLE public.tab IS NULL") {
        Statement::Comment {
            object_type,
            object_name,
            comment: None,
        } => {
            assert_eq!("public.tab", object_name.to_string());
            assert_eq!(CommentObject::Table, object_type);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_quoted_identifier() {
    pg_and_generic().verified_stmt(r#"SELECT "quoted "" ident""#);
}

#[test]
fn parse_quoted_identifier_2() {
    pg_and_generic().verified_stmt(r#"SELECT """quoted ident""""#);
}

#[test]
fn parse_local_and_global() {
    pg_and_generic().verified_stmt("CREATE LOCAL TEMPORARY TABLE table (COL INT)");
}

#[test]
fn parse_on_commit() {
    pg_and_generic()
        .verified_stmt("CREATE TEMPORARY TABLE table (COL INT) ON COMMIT PRESERVE ROWS");

    pg_and_generic().verified_stmt("CREATE TEMPORARY TABLE table (COL INT) ON COMMIT DELETE ROWS");

    pg_and_generic().verified_stmt("CREATE TEMPORARY TABLE table (COL INT) ON COMMIT DROP");
}

fn pg() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(PostgreSqlDialect {})],
    }
}

fn pg_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(PostgreSqlDialect {}), Box::new(GenericDialect {})],
    }
}

#[test]
fn parse_escaped_literal_string() {
    let sql =
        r#"SELECT E's1 \n s1', E's2 \\n s2', E's3 \\\n s3', E's4 \\\\n s4', E'\'', E'foo \\'"#;
    let select = pg_and_generic().verified_only_select(sql);
    assert_eq!(6, select.projection.len());
    assert_eq!(
        &Expr::Value(Value::EscapedStringLiteral("s1 \n s1".to_string())),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(Value::EscapedStringLiteral("s2 \\n s2".to_string())),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Value(Value::EscapedStringLiteral("s3 \\\n s3".to_string())),
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Value(Value::EscapedStringLiteral("s4 \\\\n s4".to_string())),
        expr_from_projection(&select.projection[3])
    );
    assert_eq!(
        &Expr::Value(Value::EscapedStringLiteral("'".to_string())),
        expr_from_projection(&select.projection[4])
    );
    assert_eq!(
        &Expr::Value(Value::EscapedStringLiteral("foo \\".to_string())),
        expr_from_projection(&select.projection[5])
    );

    let sql = r#"SELECT E'\'"#;
    assert_eq!(
        pg_and_generic()
            .parse_sql_statements(sql)
            .unwrap_err()
            .to_string(),
        "sql parser error: Unterminated encoded string literal at Line: 1, Column 8"
    );
}

#[test]
fn parse_declare() {
    pg_and_generic()
        .verified_stmt("DECLARE \"SQL_CUR0x7fa44801bc00\" CURSOR WITH HOLD FOR SELECT 1");
    pg_and_generic()
        .verified_stmt("DECLARE \"SQL_CUR0x7fa44801bc00\" CURSOR WITHOUT HOLD FOR SELECT 1");
    pg_and_generic().verified_stmt("DECLARE \"SQL_CUR0x7fa44801bc00\" BINARY CURSOR FOR SELECT 1");
    pg_and_generic()
        .verified_stmt("DECLARE \"SQL_CUR0x7fa44801bc00\" ASENSITIVE CURSOR FOR SELECT 1");
    pg_and_generic()
        .verified_stmt("DECLARE \"SQL_CUR0x7fa44801bc00\" INSENSITIVE CURSOR FOR SELECT 1");
    pg_and_generic().verified_stmt("DECLARE \"SQL_CUR0x7fa44801bc00\" SCROLL CURSOR FOR SELECT 1");
    pg_and_generic()
        .verified_stmt("DECLARE \"SQL_CUR0x7fa44801bc00\" NO SCROLL CURSOR FOR SELECT 1");
    pg_and_generic().verified_stmt("DECLARE \"SQL_CUR0x7fa44801bc00\" BINARY INSENSITIVE SCROLL CURSOR WITH HOLD FOR SELECT * FROM table_name LIMIT 2222");
}

#[test]
fn parse_current_functions() {
    let sql = "SELECT CURRENT_CATALOG, CURRENT_USER, SESSION_USER, USER";
    let select = pg_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("CURRENT_CATALOG")]),
            args: vec![],
            over: None,
            distinct: false,
            special: true,
        }),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("CURRENT_USER")]),
            args: vec![],
            over: None,
            distinct: false,
            special: true,
        }),
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("SESSION_USER")]),
            args: vec![],
            over: None,
            distinct: false,
            special: true,
        }),
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("USER")]),
            args: vec![],
            over: None,
            distinct: false,
            special: true,
        }),
        expr_from_projection(&select.projection[3])
    );
}

#[test]
fn parse_fetch() {
    pg_and_generic().verified_stmt("FETCH 2048 IN \"SQL_CUR0x7fa44801bc00\"");
    pg_and_generic().verified_stmt("FETCH 2048 IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic().verified_stmt("FETCH NEXT IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic().verified_stmt("FETCH PRIOR IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic().verified_stmt("FETCH FIRST IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic().verified_stmt("FETCH LAST IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic()
        .verified_stmt("FETCH ABSOLUTE 2048 IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic()
        .verified_stmt("FETCH RELATIVE 2048 IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic().verified_stmt("FETCH ALL IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic().verified_stmt("FETCH ALL IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic()
        .verified_stmt("FETCH FORWARD 2048 IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic()
        .verified_stmt("FETCH FORWARD ALL IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic()
        .verified_stmt("FETCH BACKWARD 2048 IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
    pg_and_generic()
        .verified_stmt("FETCH BACKWARD ALL IN \"SQL_CUR0x7fa44801bc00\" INTO \"new_table\"");
}

#[test]
fn parse_custom_operator() {
    // operator with a database and schema
    let sql = r#"SELECT * FROM events WHERE relname OPERATOR(database.pg_catalog.~) '^(table)$'"#;
    let select = pg().verified_only_select(sql);
    assert_eq!(
        select.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident {
                value: "relname".into(),
                quote_style: None,
            })),
            op: BinaryOperator::PGCustomBinaryOperator(vec![
                "database".into(),
                "pg_catalog".into(),
                "~".into()
            ]),
            right: Box::new(Expr::Value(Value::SingleQuotedString("^(table)$".into())))
        })
    );

    // operator with a schema
    let sql = r#"SELECT * FROM events WHERE relname OPERATOR(pg_catalog.~) '^(table)$'"#;
    let select = pg().verified_only_select(sql);
    assert_eq!(
        select.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident {
                value: "relname".into(),
                quote_style: None,
            })),
            op: BinaryOperator::PGCustomBinaryOperator(vec!["pg_catalog".into(), "~".into()]),
            right: Box::new(Expr::Value(Value::SingleQuotedString("^(table)$".into())))
        })
    );

    // custom operator without a schema
    let sql = r#"SELECT * FROM events WHERE relname OPERATOR(~) '^(table)$'"#;
    let select = pg().verified_only_select(sql);
    assert_eq!(
        select.selection,
        Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident {
                value: "relname".into(),
                quote_style: None,
            })),
            op: BinaryOperator::PGCustomBinaryOperator(vec!["~".into()]),
            right: Box::new(Expr::Value(Value::SingleQuotedString("^(table)$".into())))
        })
    );
}

#[test]
fn parse_create_role() {
    let sql = "CREATE ROLE IF NOT EXISTS mysql_a, mysql_b";
    match pg().verified_stmt(sql) {
        Statement::CreateRole {
            names,
            if_not_exists,
            ..
        } => {
            assert_eq_vec(&["mysql_a", "mysql_b"], &names);
            assert!(if_not_exists);
        }
        _ => unreachable!(),
    }

    let sql = "CREATE ROLE abc LOGIN PASSWORD NULL";
    match pg().parse_sql_statements(sql).as_deref() {
        Ok(
            [Statement::CreateRole {
                names,
                login,
                password,
                ..
            }],
        ) => {
            assert_eq_vec(&["abc"], names);
            assert_eq!(*login, Some(true));
            assert_eq!(*password, Some(Password::NullPassword));
        }
        err => panic!("Failed to parse CREATE ROLE test case: {:?}", err),
    }

    let sql = "CREATE ROLE abc WITH LOGIN PASSWORD NULL";
    match pg().parse_sql_statements(sql).as_deref() {
        Ok(
            [Statement::CreateRole {
                names,
                login,
                password,
                ..
            }],
        ) => {
            assert_eq_vec(&["abc"], names);
            assert_eq!(*login, Some(true));
            assert_eq!(*password, Some(Password::NullPassword));
        }
        err => panic!("Failed to parse CREATE ROLE test case: {:?}", err),
    }

    let sql = "CREATE ROLE magician WITH SUPERUSER CREATEROLE NOCREATEDB BYPASSRLS INHERIT PASSWORD 'abcdef' LOGIN VALID UNTIL '2025-01-01' IN ROLE role1, role2 ROLE role3 ADMIN role4, role5 REPLICATION";
    // Roundtrip order of optional parameters is not preserved
    match pg().parse_sql_statements(sql).as_deref() {
        Ok(
            [Statement::CreateRole {
                names,
                if_not_exists,
                bypassrls,
                login,
                inherit,
                password,
                superuser,
                create_db,
                create_role,
                replication,
                connection_limit,
                valid_until,
                in_role,
                in_group,
                role,
                user,
                admin,
                authorization_owner,
            }],
        ) => {
            assert_eq_vec(&["magician"], names);
            assert!(!*if_not_exists);
            assert_eq!(*login, Some(true));
            assert_eq!(*inherit, Some(true));
            assert_eq!(*bypassrls, Some(true));
            assert_eq!(
                *password,
                Some(Password::Password(Expr::Value(Value::SingleQuotedString(
                    "abcdef".into()
                ))))
            );
            assert_eq!(*superuser, Some(true));
            assert_eq!(*create_db, Some(false));
            assert_eq!(*create_role, Some(true));
            assert_eq!(*replication, Some(true));
            assert_eq!(*connection_limit, None);
            assert_eq!(
                *valid_until,
                Some(Expr::Value(Value::SingleQuotedString("2025-01-01".into())))
            );
            assert_eq_vec(&["role1", "role2"], in_role);
            assert!(in_group.is_empty());
            assert_eq_vec(&["role3"], role);
            assert!(user.is_empty());
            assert_eq_vec(&["role4", "role5"], admin);
            assert_eq!(*authorization_owner, None);
        }
        err => panic!("Failed to parse CREATE ROLE test case: {:?}", err),
    }

    let sql = "CREATE ROLE abc WITH USER foo, bar ROLE baz ";
    match pg().parse_sql_statements(sql).as_deref() {
        Ok(
            [Statement::CreateRole {
                names, user, role, ..
            }],
        ) => {
            assert_eq_vec(&["abc"], names);
            assert_eq_vec(&["foo", "bar"], user);
            assert_eq_vec(&["baz"], role);
        }
        err => panic!("Failed to parse CREATE ROLE test case: {:?}", err),
    }

    let negatables = vec![
        "BYPASSRLS",
        "CREATEDB",
        "CREATEROLE",
        "INHERIT",
        "LOGIN",
        "REPLICATION",
        "SUPERUSER",
    ];

    for negatable_kw in negatables.iter() {
        let sql = format!("CREATE ROLE abc {kw} NO{kw}", kw = negatable_kw);
        if pg().parse_sql_statements(&sql).is_ok() {
            panic!("Should not be able to parse CREATE ROLE containing both negated and non-negated versions of the same keyword: {}", negatable_kw)
        }
    }
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = pg().verified_only_select(
        r#"SELECT "alias"."bar baz", "myfun"(), "simple id" AS "column alias" FROM "a table" AS "alias""#,
    );
    // check FROM
    match only(select.from).relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
        } => {
            assert_eq!(vec![Ident::with_quote('"', "a table")], name.0);
            assert_eq!(Ident::with_quote('"', "alias"), alias.unwrap().name);
            assert!(args.is_none());
            assert!(with_hints.is_empty());
        }
        _ => panic!("Expecting TableFactor::Table"),
    }
    // check SELECT
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::CompoundIdentifier(vec![
            Ident::with_quote('"', "alias"),
            Ident::with_quote('"', "bar baz"),
        ]),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::with_quote('"', "myfun")]),
            args: vec![],
            over: None,
            distinct: false,
            special: false,
        }),
        expr_from_projection(&select.projection[1]),
    );
    match &select.projection[2] {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert_eq!(&Expr::Identifier(Ident::with_quote('"', "simple id")), expr);
            assert_eq!(&Ident::with_quote('"', "column alias"), alias);
        }
        _ => panic!("Expected ExprWithAlias"),
    }

    pg().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    pg().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = pg().verified_only_select(sql);
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            },
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' ESCAPE '\\'",
            if negated { "NOT " } else { "" }
        );
        let select = pg().verified_only_select(sql);
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            },
            select.selection.unwrap()
        );

        // This statement tests that LIKE and NOT LIKE have the same precedence.
        // This was previously mishandled (#81).
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = pg().verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            })),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_similar_to() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = pg().verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            },
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\'",
            if negated { "NOT " } else { "" }
        );
        let select = pg().verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            },
            select.selection.unwrap()
        );

        // This statement tests that SIMILAR TO and NOT SIMILAR TO have the same precedence.
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = pg().verified_only_select(sql);
        assert_eq!(
            Expr::IsNull(Box::new(Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name"))),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            })),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_create_function() {
    let sql = "CREATE FUNCTION add(INTEGER, INTEGER) RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS 'select $1 + $2;'";
    assert_eq!(
        pg().verified_stmt(sql),
        Statement::CreateFunction {
            or_replace: false,
            temporary: false,
            name: ObjectName(vec![Ident::new("add")]),
            args: Some(vec![
                OperateFunctionArg::unnamed(DataType::Integer(None)),
                OperateFunctionArg::unnamed(DataType::Integer(None)),
            ]),
            return_type: Some(DataType::Integer(None)),
            params: CreateFunctionBody {
                language: Some("SQL".into()),
                behavior: Some(FunctionBehavior::Immutable),
                as_: Some(FunctionDefinition::SingleQuotedDef(
                    "select $1 + $2;".into()
                )),
                ..Default::default()
            },
        }
    );

    let sql = "CREATE OR REPLACE FUNCTION add(a INTEGER, IN b INTEGER = 1) RETURNS INTEGER LANGUAGE SQL IMMUTABLE RETURN a + b";
    assert_eq!(
        pg().verified_stmt(sql),
        Statement::CreateFunction {
            or_replace: true,
            temporary: false,
            name: ObjectName(vec![Ident::new("add")]),
            args: Some(vec![
                OperateFunctionArg::with_name("a", DataType::Integer(None)),
                OperateFunctionArg {
                    mode: Some(ArgMode::In),
                    name: Some("b".into()),
                    data_type: DataType::Integer(None),
                    default_expr: Some(Expr::Value(Value::Number("1".parse().unwrap(), false))),
                }
            ]),
            return_type: Some(DataType::Integer(None)),
            params: CreateFunctionBody {
                language: Some("SQL".into()),
                behavior: Some(FunctionBehavior::Immutable),
                return_: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("a".into())),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Identifier("b".into())),
                }),
                ..Default::default()
            },
        }
    );

    let sql = r#"CREATE OR REPLACE FUNCTION increment(i INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$ BEGIN RETURN i + 1; END; $$"#;
    assert_eq!(
        pg().verified_stmt(sql),
        Statement::CreateFunction {
            or_replace: true,
            temporary: false,
            name: ObjectName(vec![Ident::new("increment")]),
            args: Some(vec![OperateFunctionArg::with_name(
                "i",
                DataType::Integer(None)
            )]),
            return_type: Some(DataType::Integer(None)),
            params: CreateFunctionBody {
                language: Some("plpgsql".into()),
                behavior: None,
                return_: None,
                as_: Some(FunctionDefinition::DoubleDollarDef(
                    " BEGIN RETURN i + 1; END; ".into()
                )),
                using: None
            },
        }
    );
}

#[test]
fn parse_drop_function() {
    let sql = "DROP FUNCTION IF EXISTS test_func";
    assert_eq!(
        pg().verified_stmt(sql),
        Statement::DropFunction {
            if_exists: true,
            func_desc: vec![DropFunctionDesc {
                name: ObjectName(vec![Ident {
                    value: "test_func".to_string(),
                    quote_style: None
                }]),
                args: None
            }],
            option: None
        }
    );

    let sql = "DROP FUNCTION IF EXISTS test_func(a INTEGER, IN b INTEGER = 1)";
    assert_eq!(
        pg().verified_stmt(sql),
        Statement::DropFunction {
            if_exists: true,
            func_desc: vec![DropFunctionDesc {
                name: ObjectName(vec![Ident {
                    value: "test_func".to_string(),
                    quote_style: None
                }]),
                args: Some(vec![
                    OperateFunctionArg::with_name("a", DataType::Integer(None)),
                    OperateFunctionArg {
                        mode: Some(ArgMode::In),
                        name: Some("b".into()),
                        data_type: DataType::Integer(None),
                        default_expr: Some(Expr::Value(Value::Number("1".parse().unwrap(), false))),
                    }
                ]),
            }],
            option: None
        }
    );

    let sql = "DROP FUNCTION IF EXISTS test_func1(a INTEGER, IN b INTEGER = 1), test_func2(a VARCHAR, IN b INTEGER = 1)";
    assert_eq!(
        pg().verified_stmt(sql),
        Statement::DropFunction {
            if_exists: true,
            func_desc: vec![
                DropFunctionDesc {
                    name: ObjectName(vec![Ident {
                        value: "test_func1".to_string(),
                        quote_style: None
                    }]),
                    args: Some(vec![
                        OperateFunctionArg::with_name("a", DataType::Integer(None)),
                        OperateFunctionArg {
                            mode: Some(ArgMode::In),
                            name: Some("b".into()),
                            data_type: DataType::Integer(None),
                            default_expr: Some(Expr::Value(Value::Number(
                                "1".parse().unwrap(),
                                false
                            ))),
                        }
                    ]),
                },
                DropFunctionDesc {
                    name: ObjectName(vec![Ident {
                        value: "test_func2".to_string(),
                        quote_style: None
                    }]),
                    args: Some(vec![
                        OperateFunctionArg::with_name("a", DataType::Varchar(None)),
                        OperateFunctionArg {
                            mode: Some(ArgMode::In),
                            name: Some("b".into()),
                            data_type: DataType::Integer(None),
                            default_expr: Some(Expr::Value(Value::Number(
                                "1".parse().unwrap(),
                                false
                            ))),
                        }
                    ]),
                }
            ],
            option: None
        }
    );
}

#[test]
fn parse_dollar_quoted_string() {
    let sql = "SELECT $$hello$$, $tag_name$world$tag_name$, $$Foo$Bar$$, $$Foo$Bar$$col_name, $$$$, $tag_name$$tag_name$";

    let stmt = pg().parse_sql_statements(sql).unwrap();

    let projection = match stmt.get(0).unwrap() {
        Statement::Query(query) => match &*query.body {
            SetExpr::Select(select) => &select.projection,
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };

    assert_eq!(
        &Expr::Value(Value::DollarQuotedString(DollarQuotedString {
            tag: None,
            value: "hello".into()
        })),
        expr_from_projection(&projection[0])
    );

    assert_eq!(
        &Expr::Value(Value::DollarQuotedString(DollarQuotedString {
            tag: Some("tag_name".into()),
            value: "world".into()
        })),
        expr_from_projection(&projection[1])
    );

    assert_eq!(
        &Expr::Value(Value::DollarQuotedString(DollarQuotedString {
            tag: None,
            value: "Foo$Bar".into()
        })),
        expr_from_projection(&projection[2])
    );

    assert_eq!(
        projection[3],
        SelectItem::ExprWithAlias {
            expr: Expr::Value(Value::DollarQuotedString(DollarQuotedString {
                tag: None,
                value: "Foo$Bar".into(),
            })),
            alias: Ident {
                value: "col_name".into(),
                quote_style: None,
            },
        }
    );

    assert_eq!(
        expr_from_projection(&projection[4]),
        &Expr::Value(Value::DollarQuotedString(DollarQuotedString {
            tag: None,
            value: "".into()
        })),
    );

    assert_eq!(
        expr_from_projection(&projection[5]),
        &Expr::Value(Value::DollarQuotedString(DollarQuotedString {
            tag: Some("tag_name".into()),
            value: "".into()
        })),
    );
}

#[test]
fn parse_incorrect_dollar_quoted_string() {
    let sql = "SELECT $x$hello$$";
    assert!(pg().parse_sql_statements(sql).is_err());

    let sql = "SELECT $hello$$";
    assert!(pg().parse_sql_statements(sql).is_err());

    let sql = "SELECT $$$";
    assert!(pg().parse_sql_statements(sql).is_err());
}
