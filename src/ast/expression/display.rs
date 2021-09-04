use super::*;

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Identifier(s) => write!(f, "{}", s),
            Expr::MapAccess(x) => write!(f, "{}", x),
            Expr::Wildcard => f.write_str("*"),
            Expr::QualifiedWildcard(q) => write!(f, "{}.*", display_separated(q, ".")),
            Expr::CompoundIdentifier(s) => write!(f, "{}", display_separated(s, ".")),
            Expr::IsNull(ast) => write!(f, "{} IS NULL", ast),
            Expr::IsNotNull(ast) => write!(f, "{} IS NOT NULL", ast),
            Expr::InList(x) => write!(f, "{}", x),
            Expr::InSubquery(x) => write!(f, "{}", x),
            Expr::Between(x) => write!(f, "{}", x),
            Expr::BinaryOp(x) => write!(f, "{}", x),
            Expr::UnaryOp(x) => write!(f, "{}", x),
            Expr::Cast(x) => write!(f, "{}", x),
            Expr::TryCast(x) => write!(f, "{}", x),
            Expr::Extract(x) => write!(f, "{}", x),
            Expr::Collate(x) => write!(f, "{}", x),
            Expr::Nested(ast) => write!(f, "({})", ast),
            Expr::Value(v) => write!(f, "{}", v),
            Expr::TypedString(x) => write!(f, "{}", x),
            Expr::Function(fun) => write!(f, "{}", fun),
            Expr::Case(x) => write!(f, "{}", x),
            Expr::Exists(s) => write!(f, "EXISTS ({})", s),
            Expr::Subquery(s) => write!(f, "({})", s),
            Expr::ListAgg(listagg) => write!(f, "{}", listagg),
            Expr::Substring(x) => write!(f, "{}", x),
            Expr::Trim(x) => write!(f, "{}", x),
        }
    }
}

impl fmt::Display for Trim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TRIM(")?;
        if let Some((ref ident, ref trim_char)) = self.trim_where {
            write!(f, "{} {} FROM {}", ident, trim_char, self.expr)?;
        } else {
            write!(f, "{}", self.expr)?;
        }
        write!(f, ")")
    }
}

impl fmt::Display for Substring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SUBSTRING({}", self.expr)?;
        if let Some(ref from_part) = self.substring_from {
            write!(f, " FROM {}", from_part)?;
        }
        if let Some(ref from_part) = self.substring_for {
            write!(f, " FOR {}", from_part)?;
        }

        write!(f, ")")
    }
}

impl fmt::Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CASE")?;
        if let Some(ref operand) = self.operand {
            write!(f, " {}", operand)?;
        }
        for (c, r) in self.conditions.iter().zip(&self.results) {
            write!(f, " WHEN {} THEN {}", c, r)?;
        }

        if let Some(ref else_result) = self.else_result {
            write!(f, " ELSE {}", else_result)?;
        }
        write!(f, " END")
    }
}

impl fmt::Display for TypedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data_type)?;
        write!(f, " '{}'", &value::escape_single_quote_string(&self.value))
    }
}

impl fmt::Display for Collate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} COLLATE {}", self.expr, self.collation)
    }
}
impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.op == UnaryOperator::PGPostfixFactorial {
            write!(f, "{}{}", self.expr, self.op)
        } else {
            write!(f, "{} {}", self.op, self.expr)
        }
    }
}
impl fmt::Display for InSubquery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}IN ({})",
            self.expr,
            if self.negated { "NOT " } else { "" },
            self.subquery
        )
    }
}
impl fmt::Display for Between {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}BETWEEN {} AND {}",
            self.expr,
            if self.negated { "NOT " } else { "" },
            self.low,
            self.high
        )
    }
}
impl fmt::Display for InList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}IN ({})",
            self.expr,
            if self.negated { "NOT " } else { "" },
            display_comma_separated(&self.list)
        )
    }
}
impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}
impl fmt::Display for MapAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[\"{}\"]", self.column, self.key)
    }
}

impl fmt::Display for Cast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} AS {})", self.expr, self.data_type)
    }
}

impl fmt::Display for TryCast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TRY_CAST({} AS {})", self.expr, self.data_type)
    }
}

impl fmt::Display for Extract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EXTRACT({} FROM {})", self.field, self.expr)
    }
}
