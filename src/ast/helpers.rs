#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec, vec::Vec};

use crate::ast::{
    ColumnDef, FileFormat, HiveDistributionStyle, HiveFormat, ObjectName, OnCommit, Query,
    SqlOption, Statement, TableConstraint,
};

pub struct CreateTableBuilder {
    pub or_replace: bool,
    pub temporary: bool,
    pub external: bool,
    pub global: Option<bool>,
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub hive_distribution: HiveDistributionStyle,
    pub hive_formats: Option<HiveFormat>,
    pub table_properties: Vec<SqlOption>,
    pub with_options: Vec<SqlOption>,
    pub file_format: Option<FileFormat>,
    pub location: Option<String>,
    pub query: Option<Box<Query>>,
    pub without_rowid: bool,
    pub like: Option<ObjectName>,
    pub clone: Option<ObjectName>,
    pub engine: Option<String>,
    pub default_charset: Option<String>,
    pub collation: Option<String>,
    pub on_commit: Option<OnCommit>,
    pub on_cluster: Option<String>,
}

impl CreateTableBuilder {
    pub fn new(name: ObjectName) -> Self {
        CreateTableBuilder {
            or_replace: false,
            temporary: false,
            external: false,
            global: None,
            if_not_exists: false,
            name,
            columns: vec![],
            constraints: vec![],
            hive_distribution: HiveDistributionStyle::NONE,
            hive_formats: None,
            table_properties: vec![],
            with_options: vec![],
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
            like: None,
            clone: None,
            engine: None,
            default_charset: None,
            collation: None,
            on_commit: None,
            on_cluster: None,
        }
    }
    pub fn or_replace(mut self, or_replace: bool) -> Self {
        self.or_replace = or_replace;
        self
    }

    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    pub fn external(mut self, external: bool) -> Self {
        self.external = external;
        self
    }

    pub fn global(mut self, global: Option<bool>) -> Self {
        self.global = global;
        self
    }

    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    pub fn columns(mut self, columns: Vec<ColumnDef>) -> Self {
        self.columns = columns;
        self
    }

    pub fn constraints(mut self, constraints: Vec<TableConstraint>) -> Self {
        self.constraints = constraints;
        self
    }

    pub fn hive_distribution(mut self, hive_distribution: HiveDistributionStyle) -> Self {
        self.hive_distribution = hive_distribution;
        self
    }

    pub fn hive_formats(mut self, hive_formats: Option<HiveFormat>) -> Self {
        self.hive_formats = hive_formats;
        self
    }

    pub fn table_properties(mut self, table_properties: Vec<SqlOption>) -> Self {
        self.table_properties = table_properties;
        self
    }

    pub fn with_options(mut self, with_options: Vec<SqlOption>) -> Self {
        self.with_options = with_options;
        self
    }
    pub fn file_format(mut self, file_format: Option<FileFormat>) -> Self {
        self.file_format = file_format;
        self
    }
    pub fn location(mut self, location: Option<String>) -> Self {
        self.location = location;
        self
    }

    pub fn query(mut self, query: Option<Box<Query>>) -> Self {
        self.query = query;
        self
    }
    pub fn without_rowid(mut self, without_rowid: bool) -> Self {
        self.without_rowid = without_rowid;
        self
    }

    pub fn like(mut self, like: Option<ObjectName>) -> Self {
        self.like = like;
        self
    }

    pub fn clone(mut self, clone: Option<ObjectName>) -> Self {
        self.clone = clone;
        self
    }

    pub fn engine(mut self, engine: Option<String>) -> Self {
        self.engine = engine;
        self
    }

    pub fn default_charset(mut self, default_charset: Option<String>) -> Self {
        self.default_charset = default_charset;
        self
    }

    pub fn collation(mut self, collation: Option<String>) -> Self {
        self.collation = collation;
        self
    }

    pub fn on_commit(mut self, on_commit: Option<OnCommit>) -> Self {
        self.on_commit = on_commit;
        self
    }

    pub fn on_cluster(mut self, on_cluster: Option<String>) -> Self {
        self.on_cluster = on_cluster;
        self
    }

    pub fn build(self) -> Statement {
        Statement::CreateTable {
            or_replace: self.or_replace,
            temporary: self.temporary,
            external: self.external,
            global: self.global,
            if_not_exists: self.if_not_exists,
            name: self.name,
            columns: self.columns,
            constraints: self.constraints,
            hive_distribution: self.hive_distribution,
            hive_formats: self.hive_formats,
            table_properties: self.table_properties,
            with_options: self.with_options,
            file_format: self.file_format,
            location: self.location,
            query: self.query,
            without_rowid: self.without_rowid,
            like: self.like,
            clone: self.clone,
            engine: self.engine,
            default_charset: self.default_charset,
            collation: self.collation,
            on_commit: self.on_commit,
            on_cluster: self.on_cluster,
        }
    }
}
