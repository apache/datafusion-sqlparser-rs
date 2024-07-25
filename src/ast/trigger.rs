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

//! SQL Abstract Syntax Tree (AST) for triggers.
use super::*;

/// Function describe in DROP FUNCTION.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FunctionDesc {
    pub name: ObjectName,
    pub args: Vec<OperateFunctionArg>,
}

impl fmt::Display for FunctionDesc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({})", self.name, display_comma_separated(&self.args))
    }
}

/// This specifies whether the trigger function should be fired once for every row affected by the trigger event, or just once per SQL statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TriggerObject {
    Row,
    Statement,
}

impl fmt::Display for TriggerObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TriggerObject::Row => write!(f, "ROW"),
            TriggerObject::Statement => write!(f, "STATEMENT"),
        }
    }
}

/// This clause indicates whether the following relation name is for the before-image transition relation or the after-image transition relation
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TriggerReferencingType {
    OldTable,
    NewTable,
}

impl fmt::Display for TriggerReferencingType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TriggerReferencingType::OldTable => write!(f, "OLD TABLE"),
            TriggerReferencingType::NewTable => write!(f, "NEW TABLE"),
        }
    }
}

/// This keyword immediately precedes the declaration of one or two relation names that provide access to the transition relations of the triggering statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TriggerReferencing {
    pub refer_type: TriggerReferencingType,
    pub is_as: bool,
    pub transition_relation_name: ObjectName,
}

impl fmt::Display for TriggerReferencing {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{refer_type}{is_as} {relation_name}",
            refer_type = self.refer_type,
            is_as = if self.is_as { " AS" } else { "" },
            relation_name = self.transition_relation_name
        )
    }
}

/// Used to describe trigger events
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TriggerEvent {
    Insert,
    Update(Vec<Ident>),
    Delete,
    Truncate,
}

impl fmt::Display for TriggerEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TriggerEvent::Insert => write!(f, "INSERT"),
            TriggerEvent::Update(columns) => {
                write!(f, "UPDATE")?;
                if !columns.is_empty() {
                    write!(f, " OF")?;
                    write!(f, " {}", display_comma_separated(columns))?;
                }
                Ok(())
            }
            TriggerEvent::Delete => write!(f, "DELETE"),
            TriggerEvent::Truncate => write!(f, "TRUNCATE"),
        }
    }
}

/// Trigger period
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TriggerPeriod {
    After,
    Before,
    InsteadOf,
}

impl fmt::Display for TriggerPeriod {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TriggerPeriod::After => write!(f, "AFTER"),
            TriggerPeriod::Before => write!(f, "BEFORE"),
            TriggerPeriod::InsteadOf => write!(f, "INSTEAD OF"),
        }
    }
}

/// Execute function or stored procedure
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ExecBodyType {
    Function,
    Proceduer,
}

impl fmt::Display for ExecBodyType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExecBodyType::Function => write!(f, "FUNCTION"),
            ExecBodyType::Proceduer => write!(f, "PROCEDURE"),
        }
    }
}
/// This keyword immediately precedes the declaration of one or two relation names that provide access to the transition relations of the triggering statement
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TriggerExecBody {
    pub exec_type: ExecBodyType,
    pub func_desc: FunctionDesc,
}

impl fmt::Display for TriggerExecBody {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{exec_type} {func_desc}",
            exec_type = self.exec_type,
            func_desc = self.func_desc
        )
    }
}
