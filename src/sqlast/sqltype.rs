
/// SQL datatypes for literals in SQL statements
#[derive(Debug, Clone, PartialEq)]
pub enum SQLType {
    /// Fixed-length character type e.g. CHAR(10)
    Char(Option<usize>),
    /// Variable-length character type e.g. VARCHAR(10)
    Varchar(Option<usize>),
    /// Uuid type
    Uuid,
    /// Large character object e.g. CLOB(1000)
    Clob(usize),
    /// Fixed-length binary type e.g. BINARY(10)
    Binary(usize),
    /// Variable-length binary type e.g. VARBINARY(10)
    Varbinary(usize),
    /// Large binary object e.g. BLOB(1000)
    Blob(usize),
    /// Decimal type with precision and optional scale e.g. DECIMAL(10,2)
    Decimal(usize, Option<usize>),
    /// Small integer
    SmallInt,
    /// Integer
    Int,
    /// Big integer
    BigInt,
    /// Floating point with optional precision e.g. FLOAT(8)
    Float(Option<usize>),
    /// Floating point e.g. REAL
    Real,
    /// Double e.g. DOUBLE PRECISION
    Double,
    /// Boolean
    Boolean,
    /// Date
    Date,
    /// Time
    Time,
    /// Timestamp
    Timestamp,
    /// Regclass used in postgresql serial
    Regclass,
    /// Text
    Text,
    /// Bytea
    Bytea,
    /// Custom type such as enums
    Custom(String),
    /// Arrays
    Array(Box<SQLType>),
}

impl ToString for SQLType{

    fn to_string(&self) -> String {
        match self {
            SQLType::Char(size) => if let Some(size) = size {
                format!("char({})", size)
            }else{
                "char".to_string()
            }
            SQLType::Varchar(size) => if let Some(size) = size{
                format!("character varying({})", size)
            }else{
                "character varying".to_string()
            }
            SQLType::Uuid => "uuid".to_string(),
            SQLType::Clob(size) => format!("clob({})", size),
            SQLType::Binary(size) => format!("binary({})", size),
            SQLType::Varbinary(size) => format!("varbinary({})", size),
            SQLType::Blob(size) => format!("blob({})", size),
            SQLType::Decimal(precision, scale) => if let Some(scale) = scale{
                format!("numeric({},{})", precision, scale)
            }else{
                format!("numeric({})", precision)
            },
            SQLType::SmallInt => "smallint".to_string(),
            SQLType::Int => "int".to_string(),
            SQLType::BigInt => "bigint".to_string(),
            SQLType::Float(size) => if let Some(size) = size{
                format!("float({})", size)
            }else{
                "float".to_string()
            },
            SQLType::Real => "real".to_string(),
            SQLType::Double => "double".to_string(),
            SQLType::Boolean => "boolean".to_string(),
            SQLType::Date => "date".to_string(),
            SQLType::Time => "time".to_string(),
            SQLType::Timestamp => "timestamp".to_string(),
            SQLType::Regclass => "regclass".to_string(),
            SQLType::Text => "text".to_string(),
            SQLType::Bytea => "bytea".to_string(),
            SQLType::Array(ty) => format!("{}[]",ty.to_string()),
            SQLType::Custom(ty) => ty.to_string(),
        }
    }
}
