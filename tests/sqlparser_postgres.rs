use log::debug;

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::sqlast::*;
use sqlparser::sqlparser::*;
use sqlparser::sqltokenizer::*;

#[test]
fn test_prev_index() {
    let sql: &str = "SELECT version()";
    let mut parser = parser(sql);
    assert_eq!(parser.prev_token(), None);
    assert_eq!(parser.next_token(), Some(Token::make_keyword("SELECT")));
    assert_eq!(parser.next_token(), Some(Token::make_word("version", None)));
    assert_eq!(parser.prev_token(), Some(Token::make_word("version", None)));
    assert_eq!(parser.peek_token(), Some(Token::make_word("version", None)));
    assert_eq!(parser.prev_token(), Some(Token::make_keyword("SELECT")));
    assert_eq!(parser.prev_token(), None);
}

#[test]
fn parse_simple_insert() {
    let sql = String::from("INSERT INTO customer VALUES(1, 2, 3)");
    match verified_stmt(&sql) {
        SQLStatement::SQLInsert {
            table_name,
            columns,
            values,
            ..
        } => {
            assert_eq!(table_name.to_string(), "customer");
            assert!(columns.is_empty());
            assert_eq!(
                vec![vec![
                    ASTNode::SQLValue(Value::Long(1)),
                    ASTNode::SQLValue(Value::Long(2)),
                    ASTNode::SQLValue(Value::Long(3))
                ]],
                values
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_common_insert() {
    let sql = String::from("INSERT INTO public.customer VALUES(1, 2, 3)");
    match verified_stmt(&sql) {
        SQLStatement::SQLInsert {
            table_name,
            columns,
            values,
            ..
        } => {
            assert_eq!(table_name.to_string(), "public.customer");
            assert!(columns.is_empty());
            assert_eq!(
                vec![vec![
                    ASTNode::SQLValue(Value::Long(1)),
                    ASTNode::SQLValue(Value::Long(2)),
                    ASTNode::SQLValue(Value::Long(3))
                ]],
                values
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_complex_insert() {
    let sql = String::from("INSERT INTO db.public.customer VALUES(1, 2, 3)");
    match verified_stmt(&sql) {
        SQLStatement::SQLInsert {
            table_name,
            columns,
            values,
            ..
        } => {
            assert_eq!(table_name.to_string(), "db.public.customer");
            assert!(columns.is_empty());
            assert_eq!(
                vec![vec![
                    ASTNode::SQLValue(Value::Long(1)),
                    ASTNode::SQLValue(Value::Long(2)),
                    ASTNode::SQLValue(Value::Long(3))
                ]],
                values
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_invalid_table_name() {
    let mut parser = parser("db.public..customer");
    let ast = parser.parse_object_name();
    assert!(ast.is_err());
}

#[test]
fn parse_no_table_name() {
    let mut parser = parser("");
    let ast = parser.parse_object_name();
    assert!(ast.is_err());
}

#[test]
fn parse_insert_with_columns() {
    let sql = String::from("INSERT INTO public.customer (id, name, active) VALUES(1, 2, 3)");
    match verified_stmt(&sql) {
        SQLStatement::SQLInsert {
            table_name,
            columns,
            values,
            ..
        } => {
            assert_eq!(table_name.to_string(), "public.customer");
            assert_eq!(
                columns,
                vec!["id".to_string(), "name".to_string(), "active".to_string()]
            );
            assert_eq!(
                vec![vec![
                    ASTNode::SQLValue(Value::Long(1)),
                    ASTNode::SQLValue(Value::Long(2)),
                    ASTNode::SQLValue(Value::Long(3))
                ]],
                values
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_insert_invalid() {
    let sql = String::from("INSERT public.customer (id, name, active) VALUES (1, 2, 3)");
    match Parser::parse_sql(&PostgreSqlDialect {}, sql) {
        Err(_) => {}
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_defaults() {
    let sql = String::from(
        "CREATE TABLE public.customer (
            customer_id integer DEFAULT nextval(public.customer_customer_id_seq) NOT NULL,
            store_id smallint NOT NULL,
            first_name character varying(45) NOT NULL,
            last_name character varying(45) NOT NULL,
            email character varying(50),
            address_id smallint NOT NULL,
            activebool boolean DEFAULT true NOT NULL,
            create_date date DEFAULT now()::text NOT NULL,
            last_update timestamp without time zone DEFAULT now() NOT NULL,
            active integer NOT NULL)",
    );
    match one_statement_parses_to(&sql, "") {
        SQLStatement::SQLCreateTable {
            name,
            columns,
            external: false,
            file_format: None,
            location: None,
        } => {
            assert_eq!("public.customer", name.to_string());
            assert_eq!(10, columns.len());

            let c_name = &columns[0];
            assert_eq!("customer_id", c_name.name);
            assert_eq!(SQLType::Int, c_name.data_type);
            assert_eq!(false, c_name.allow_null);

            let c_lat = &columns[1];
            assert_eq!("store_id", c_lat.name);
            assert_eq!(SQLType::SmallInt, c_lat.data_type);
            assert_eq!(false, c_lat.allow_null);

            let c_lng = &columns[2];
            assert_eq!("first_name", c_lng.name);
            assert_eq!(SQLType::Varchar(Some(45)), c_lng.data_type);
            assert_eq!(false, c_lng.allow_null);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_from_pg_dump() {
    let sql = String::from("
        CREATE TABLE public.customer (
            customer_id integer DEFAULT nextval('public.customer_customer_id_seq'::regclass) NOT NULL,
            store_id smallint NOT NULL,
            first_name character varying(45) NOT NULL,
            last_name character varying(45) NOT NULL,
            info text[],
            address_id smallint NOT NULL,
            activebool boolean DEFAULT true NOT NULL,
            create_date date DEFAULT now()::date NOT NULL,
            create_date1 date DEFAULT 'now'::text::date NOT NULL,
            last_update timestamp without time zone DEFAULT now(),
            release_year public.year,
            active integer
        )");
    match one_statement_parses_to(&sql, "") {
        SQLStatement::SQLCreateTable {
            name,
            columns,
            external: false,
            file_format: None,
            location: None,
        } => {
            assert_eq!("public.customer", name.to_string());

            let c_customer_id = &columns[0];
            assert_eq!("customer_id", c_customer_id.name);
            assert_eq!(SQLType::Int, c_customer_id.data_type);
            assert_eq!(false, c_customer_id.allow_null);

            let c_store_id = &columns[1];
            assert_eq!("store_id", c_store_id.name);
            assert_eq!(SQLType::SmallInt, c_store_id.data_type);
            assert_eq!(false, c_store_id.allow_null);

            let c_first_name = &columns[2];
            assert_eq!("first_name", c_first_name.name);
            assert_eq!(SQLType::Varchar(Some(45)), c_first_name.data_type);
            assert_eq!(false, c_first_name.allow_null);

            let c_create_date1 = &columns[8];
            assert_eq!(
                Some(ASTNode::SQLCast {
                    expr: Box::new(ASTNode::SQLCast {
                        expr: Box::new(ASTNode::SQLValue(Value::SingleQuotedString(
                            "now".to_string()
                        ))),
                        data_type: SQLType::Text
                    }),
                    data_type: SQLType::Date
                }),
                c_create_date1.default
            );

            let c_release_year = &columns[10];
            assert_eq!(
                SQLType::Custom(SQLObjectName(vec![
                    "public".to_string(),
                    "year".to_string()
                ])),
                c_release_year.data_type
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_inherit() {
    let sql = String::from(
        "\
         CREATE TABLE bazaar.settings (\
         settings_id uuid PRIMARY KEY DEFAULT uuid_generate_v4() NOT NULL, \
         user_id uuid UNIQUE, \
         value text[], \
         use_metric boolean DEFAULT true\
         )",
    );
    match verified_stmt(&sql) {
        SQLStatement::SQLCreateTable {
            name,
            columns,
            external: false,
            file_format: None,
            location: None,
        } => {
            assert_eq!("bazaar.settings", name.to_string());

            let c_name = &columns[0];
            assert_eq!("settings_id", c_name.name);
            assert_eq!(SQLType::Uuid, c_name.data_type);
            assert_eq!(false, c_name.allow_null);
            assert_eq!(true, c_name.is_primary);
            assert_eq!(false, c_name.is_unique);

            let c_name = &columns[1];
            assert_eq!("user_id", c_name.name);
            assert_eq!(SQLType::Uuid, c_name.data_type);
            assert_eq!(true, c_name.allow_null);
            assert_eq!(false, c_name.is_primary);
            assert_eq!(true, c_name.is_unique);
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_constraint_primary_key() {
    let sql = String::from(
        "\
         ALTER TABLE bazaar.address \
         ADD CONSTRAINT address_pkey PRIMARY KEY (address_id)",
    );
    match verified_stmt(&sql) {
        SQLStatement::SQLAlterTable { name, .. } => {
            assert_eq!(name.to_string(), "bazaar.address");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_alter_table_constraint_foreign_key() {
    let sql = String::from("\
    ALTER TABLE public.customer \
        ADD CONSTRAINT customer_address_id_fkey FOREIGN KEY (address_id) REFERENCES public.address(address_id)");
    match verified_stmt(&sql) {
        SQLStatement::SQLAlterTable { name, .. } => {
            assert_eq!(name.to_string(), "public.customer");
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_copy_example() {
    let sql = String::from(r#"COPY public.actor (actor_id, first_name, last_name, last_update, value) FROM stdin;
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
\\."#);
    let ast = one_statement_parses_to(&sql, "");
    println!("{:#?}", ast);
    //assert_eq!(sql, ast.to_string());
}

#[test]
fn parse_timestamps_example() {
    let sql = "2016-02-15 09:43:33";
    let _ = parse_sql_expr(sql);
    //TODO add assertion
    //assert_eq!(sql, ast.to_string());
}

#[test]
fn parse_timestamps_with_millis_example() {
    let sql = "2017-11-02 19:15:42.308637";
    let _ = parse_sql_expr(sql);
    //TODO add assertion
    //assert_eq!(sql, ast.to_string());
}

#[test]
fn parse_example_value() {
    let sql = "SARAH.LEWIS@sakilacustomer.org";
    let ast = parse_sql_expr(sql);
    assert_eq!(sql, ast.to_string());
}

#[test]
fn parse_function_now() {
    let sql = "now()";
    let ast = parse_sql_expr(sql);
    assert_eq!(sql, ast.to_string());
}

fn verified_stmt(query: &str) -> SQLStatement {
    one_statement_parses_to(query, query)
}

/// Ensures that `sql` parses as a single statement, optionally checking that
/// converting AST back to string equals to `canonical` (unless an empty string
/// is provided).
fn one_statement_parses_to(sql: &str, canonical: &str) -> SQLStatement {
    let mut statements = parse_sql_statements(&sql).unwrap();
    assert_eq!(statements.len(), 1);

    let only_statement = statements.pop().unwrap();
    if !canonical.is_empty() {
        assert_eq!(canonical, only_statement.to_string())
    }
    only_statement
}

fn parse_sql_statements(sql: &str) -> Result<Vec<SQLStatement>, ParserError> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql.to_string())
}

fn parse_sql_expr(sql: &str) -> ASTNode {
    debug!("sql: {}", sql);
    let mut parser = parser(sql);
    parser.parse_expr().unwrap()
}

fn parser(sql: &str) -> Parser {
    let dialect = PostgreSqlDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, &sql);
    let tokens = tokenizer.tokenize().unwrap();
    debug!("tokens: {:#?}", tokens);
    Parser::new(tokens)
}
