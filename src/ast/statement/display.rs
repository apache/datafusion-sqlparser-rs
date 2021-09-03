use super::*;

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Statement::*;
        match self {
            Analyze(x) => write!(f, "{};", x),
            Truncate(x) => write!(f, "{};", x),
            Explain(x) => write!(f, "{};", x),
            Query(x) => write!(f, "{};", x),
            Msck(x) => write!(f, "{};", x),
            Insert(x) => write!(f, "{};", x),
            Directory(x) => write!(f, "{};", x),
            Copy(x) => write!(f, "{};", x),
            Update(x) => write!(f, "{};", x),
            Delete(x) => write!(f, "{};", x),
            CreateView(x) => write!(f, "{};", x),
            CreateTable(x) => write!(f, "{};", x),
            CreateVirtualTable(x) => write!(f, "{};", x),
            CreateIndex(x) => write!(f, "{};", x),
            AlterTable(x) => write!(f, "{};", x),
            SetVariable(x) => write!(f, "{};", x),
            ShowVariable(x) => write!(f, "{};", x),
            ShowCreate(x) => write!(f, "{};", x),
            ShowColumns(x) => write!(f, "{};", x),
            StartTransaction(x) => write!(f, "{};", x),
            SetTransaction(x) => write!(f, "{};", x),
            Rollback(x) => write!(f, "{};", x),
            Drop(x) => write!(f, "{};", x),
            Commit(x) => write!(f, "{};", x),
            CreateSchema(x) => write!(f, "{};", x),
            CreateDatabase(x) => write!(f, "{};", x),
            Assert(x) => write!(f, "{};", x),
            Deallocate(x) => write!(f, "{};", x),
            Execute(x) => write!(f, "{};", x),
            Prepare(x) => write!(f, "{};", x),
        }
    }
}
impl fmt::Display for Explain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXPLAIN ")?;

        if self.analyze {
            write!(f, "ANALYZE ")?;
        }

        if self.verbose {
            write!(f, "VERBOSE ")?;
        }

        write!(f, "{}", self.statement)
    }
}

impl fmt::Display for Directory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "INSERT{overwrite}{local} DIRECTORY '{path}'",
            overwrite = if self.overwrite { " OVERWRITE" } else { "" },
            local = if self.local { " LOCAL" } else { "" },
            path = self.path
        )?;
        if let Some(ref ff) = self.file_format {
            write!(f, " STORED AS {}", ff)?
        }
        write!(f, " {}", self.source)
    }
}

impl fmt::Display for Msck {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MSCK {repair}TABLE {table}",
            repair = if self.repair { "REPAIR " } else { "" },
            table = self.table_name
        )?;
        if let Some(ref pa) = self.partition_action {
            write!(f, " {}", pa)?;
        }
        Ok(())
    }
}

impl fmt::Display for Truncate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TRUNCATE TABLE {}", self.table_name)?;
        if let Some(ref parts) = self.partitions {
            if !parts.is_empty() {
                write!(f, " PARTITION ({})", display_comma_separated(parts))?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for Analyze {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ANALYZE TABLE {}", self.table_name)?;
        if let Some(ref parts) = self.partitions {
            if !parts.is_empty() {
                write!(f, " PARTITION ({})", display_comma_separated(parts))?;
            }
        }

        if self.compute_statistics {
            write!(f, " COMPUTE STATISTICS")?;
        }
        if self.noscan {
            write!(f, " NOSCAN")?;
        }
        if self.cache_metadata {
            write!(f, " CACHE METADATA")?;
        }
        if self.for_columns {
            write!(f, " FOR COLUMNS")?;
            if !self.columns.is_empty() {
                write!(f, " {}", display_comma_separated(&self.columns))?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for Insert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(action) = &self.or {
            write!(f, "INSERT OR {} INTO {} ", action, self.table_name)?;
        } else {
            write!(
                f,
                "INSERT {act}{tbl} {table_name} ",
                table_name = self.table_name,
                act = if self.overwrite { "OVERWRITE" } else { "INTO" },
                tbl = if self.table { " TABLE" } else { "" }
            )?;
        }
        if !self.columns.is_empty() {
            write!(f, "({}) ", display_comma_separated(&self.columns))?;
        }
        if let Some(ref parts) = self.partitioned {
            if !parts.is_empty() {
                write!(f, "PARTITION ({}) ", display_comma_separated(parts))?;
            }
        }
        if !self.after_columns.is_empty() {
            write!(f, "({}) ", display_comma_separated(&self.after_columns))?;
        }
        write!(f, "{}", self.source)
    }
}

impl fmt::Display for Copy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "COPY {}", self.table_name)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.columns))?;
        }
        write!(f, " FROM stdin; ")?;
        if !self.values.is_empty() {
            writeln!(f)?;
            let mut delim = "";
            for v in &self.values {
                write!(f, "{}", delim)?;
                delim = "\t";
                if let Some(v) = v {
                    write!(f, "{}", v)?;
                } else {
                    write!(f, "\\N")?;
                }
            }
        }
        write!(f, "\n\\.")
    }
}

impl fmt::Display for Update {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UPDATE {}", self.table_name)?;
        if !self.assignments.is_empty() {
            write!(f, " SET {}", display_comma_separated(&self.assignments))?;
        }
        if let Some(ref selection) = self.selection {
            write!(f, " WHERE {}", selection)?;
        }
        Ok(())
    }
}

impl fmt::Display for Delete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DELETE FROM {}", self.table_name)?;
        if let Some(ref selection) = self.selection {
            write!(f, " WHERE {}", selection)?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateDatabase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.db_name)?;
        if let Some(ref l) = self.location {
            write!(f, " LOCATION '{}'", l)?;
        }
        if let Some(ref ml) = self.managed_location {
            write!(f, " MANAGEDLOCATION '{}'", ml)?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateView {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE {or_replace}{materialized}VIEW {name}",
            or_replace = if self.or_replace { "OR REPLACE " } else { "" },
            materialized = if self.materialized {
                "MATERIALIZED "
            } else {
                ""
            },
            name = self.name
        )?;
        if !self.with_options.is_empty() {
            write!(f, " WITH ({})", display_comma_separated(&self.with_options))?;
        }
        if !self.columns.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.columns))?;
        }
        write!(f, " AS {}", self.query)
    }
}

impl fmt::Display for CreateTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ref name = self.name;
        let ref columns = self.columns;
        let ref constraints = self.constraints;
        let ref table_properties = self.table_properties;
        let ref with_options = self.with_options;
        let ref or_replace = self.or_replace;
        let ref if_not_exists = self.if_not_exists;
        let ref hive_distribution = self.hive_distribution;
        let ref hive_formats = self.hive_formats;
        let ref external = self.external;
        let ref temporary = self.temporary;
        let ref file_format = self.file_format;
        let ref location = self.location;
        let ref query = self.query;
        let ref without_rowid = self.without_rowid;
        let ref like = self.like;
        write!(
            f,
            "CREATE {or_replace}{external}{temporary}TABLE {if_not_exists}{name}",
            or_replace = if *or_replace { "OR REPLACE " } else { "" },
            external = if *external { "EXTERNAL " } else { "" },
            if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
            temporary = if *temporary { "TEMPORARY " } else { "" },
            name = name,
        )?;
        if !columns.is_empty() || !constraints.is_empty() {
            write!(f, " ({}", display_comma_separated(columns))?;
            if !columns.is_empty() && !constraints.is_empty() {
                write!(f, ", ")?;
            }
            write!(f, "{})", display_comma_separated(constraints))?;
        } else if query.is_none() && like.is_none() {
            // PostgreSQL allows `CREATE TABLE t ();`, but requires empty parens
            write!(f, " ()")?;
        }
        // Only for SQLite
        if *without_rowid {
            write!(f, " WITHOUT ROWID")?;
        }

        // Only for Hive
        if let Some(l) = like {
            write!(f, " LIKE {}", l)?;
        }
        match hive_distribution {
            HiveDistributionStyle::PARTITIONED { columns } => {
                write!(f, " PARTITIONED BY ({})", display_comma_separated(columns))?;
            }
            HiveDistributionStyle::CLUSTERED {
                columns,
                sorted_by,
                num_buckets,
            } => {
                write!(f, " CLUSTERED BY ({})", display_comma_separated(columns))?;
                if !sorted_by.is_empty() {
                    write!(f, " SORTED BY ({})", display_comma_separated(sorted_by))?;
                }
                if *num_buckets > 0 {
                    write!(f, " INTO {} BUCKETS", num_buckets)?;
                }
            }
            HiveDistributionStyle::SKEWED {
                columns,
                on,
                stored_as_directories,
            } => {
                write!(
                    f,
                    " SKEWED BY ({})) ON ({})",
                    display_comma_separated(columns),
                    display_comma_separated(on)
                )?;
                if *stored_as_directories {
                    write!(f, " STORED AS DIRECTORIES")?;
                }
            }
            _ => (),
        }

        if let Some(HiveFormat {
            row_format,
            storage,
            location,
        }) = hive_formats
        {
            match row_format {
                Some(HiveRowFormat::SERDE { class }) => write!(f, " ROW FORMAT SERDE '{}'", class)?,
                Some(HiveRowFormat::DELIMITED) => write!(f, " ROW FORMAT DELIMITED")?,
                None => (),
            }
            match storage {
                Some(HiveIOFormat::IOF {
                    input_format,
                    output_format,
                }) => write!(
                    f,
                    " STORED AS INPUTFORMAT {} OUTPUTFORMAT {}",
                    input_format, output_format
                )?,
                Some(HiveIOFormat::FileFormat { format }) if !*external => {
                    write!(f, " STORED AS {}", format)?
                }
                _ => (),
            }
            if !*external {
                if let Some(loc) = location {
                    write!(f, " LOCATION '{}'", loc)?;
                }
            }
        }
        if *external {
            write!(
                f,
                " STORED AS {} LOCATION '{}'",
                file_format.as_ref().unwrap(),
                location.as_ref().unwrap()
            )?;
        }
        if !table_properties.is_empty() {
            write!(
                f,
                " TBLPROPERTIES ({})",
                display_comma_separated(table_properties)
            )?;
        }
        if !with_options.is_empty() {
            write!(f, " WITH ({})", display_comma_separated(with_options))?;
        }
        if let Some(query) = query {
            write!(f, " AS {}", query)?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateVirtualTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE VIRTUAL TABLE {if_not_exists}{name} USING {module_name}",
            if_not_exists = if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            name = self.name,
            module_name = self.module_name
        )?;
        if !self.module_args.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.module_args))?;
        }
        Ok(())
    }
}

impl fmt::Display for CreateIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE {unique}INDEX {if_not_exists}{name} ON {table_name}({columns})",
            unique = if self.unique { "UNIQUE " } else { "" },
            if_not_exists = if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            name = self.name,
            table_name = self.table_name,
            columns = display_separated(&self.columns, ",")
        )
    }
}

impl fmt::Display for AlterTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ALTER TABLE {} {}", self.name, self.operation)
    }
}

impl fmt::Display for Drop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DROP {}{} {}{}{}",
            self.object_type,
            if self.if_exists { " IF EXISTS" } else { "" },
            display_comma_separated(&self.names),
            if self.cascade { " CASCADE" } else { "" },
            if self.purge { " PURGE" } else { "" }
        )
    }
}

impl fmt::Display for SetVariable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("SET ")?;
        if self.local {
            f.write_str("LOCAL ")?;
        }
        write!(
            f,
            "{hivevar}{name} = {value}",
            hivevar = if self.hivevar { "HIVEVAR:" } else { "" },
            name = self.variable,
            value = display_comma_separated(&self.value)
        )
    }
}

impl fmt::Display for ShowVariable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SHOW")?;
        if !self.variable.is_empty() {
            write!(f, " {}", display_separated(&self.variable, " "))?;
        }
        Ok(())
    }
}

impl fmt::Display for ShowCreate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SHOW CREATE {obj_type} {obj_name}",
            obj_type = self.obj_type,
            obj_name = self.obj_name,
        )?;
        Ok(())
    }
}

impl fmt::Display for ShowColumns {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SHOW {extended}{full}COLUMNS FROM {table_name}",
            extended = if self.extended { "EXTENDED " } else { "" },
            full = if self.full { "FULL " } else { "" },
            table_name = self.table_name,
        )?;
        if let Some(filter) = &self.filter {
            write!(f, " {}", filter)?;
        }
        Ok(())
    }
}

impl fmt::Display for StartTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "START TRANSACTION")?;
        if !self.modes.is_empty() {
            write!(f, " {}", display_comma_separated(&self.modes))?;
        }
        Ok(())
    }
}

impl fmt::Display for SetTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SET TRANSACTION")?;
        if !self.modes.is_empty() {
            write!(f, " {}", display_comma_separated(&self.modes))?;
        }
        Ok(())
    }
}

impl fmt::Display for Commit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "COMMIT{}", if self.chain { " AND CHAIN" } else { "" },)
    }
}

impl fmt::Display for Rollback {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ROLLBACK{}", if self.chain { " AND CHAIN" } else { "" },)
    }
}

impl fmt::Display for CreateSchema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE SCHEMA {if_not_exists}{name}",
            if_not_exists = if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            name = self.schema_name
        )
    }
}

impl fmt::Display for Assert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ASSERT {}", self.condition)?;
        if let Some(m) = &self.message {
            write!(f, " AS {}", m)?;
        }
        Ok(())
    }
}
impl fmt::Display for Deallocate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DEALLOCATE {prepare}{name}",
            prepare = if self.prepare { "PREPARE " } else { "" },
            name = self.name,
        )
    }
}

impl fmt::Display for Execute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXECUTE {}", self.name)?;
        if !self.parameters.is_empty() {
            write!(f, "({})", display_comma_separated(&self.parameters))?;
        }
        Ok(())
    }
}

impl fmt::Display for Prepare {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PREPARE {} ", self.name)?;
        if !self.data_types.is_empty() {
            write!(f, "({}) ", display_comma_separated(&self.data_types))?;
        }
        write!(f, "AS {}", self.statement)
    }
}
