// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Implementations of the `From` trait to convert from various
//! AST nodes to `Statement` nodes.

use crate::ast::{
    AlterSchema, AlterType, CaseStatement, CreateConnector, CreateDomain, CreateFunction,
    CreateIndex, CreateServerStatement, CreateTable, CreateTrigger, CreateUser, Delete,
    DenyStatement, DropDomain, DropTrigger, ExportData, Function, IfStatement, Insert,
    OpenStatement, PrintStatement, Query, RaiseStatement, RenameTable, ReturnStatement, Set,
    ShowCharset, ShowObjects, Statement, Use, VacuumStatement, WhileStatement,
};

impl From<Set> for Statement {
    fn from(s: Set) -> Self {
        Self::Set(s)
    }
}

impl From<Query> for Statement {
    fn from(q: Query) -> Self {
        Box::new(q).into()
    }
}

impl From<Box<Query>> for Statement {
    fn from(q: Box<Query>) -> Self {
        Self::Query(q)
    }
}

impl From<Insert> for Statement {
    fn from(i: Insert) -> Self {
        Self::Insert(i)
    }
}

impl From<CaseStatement> for Statement {
    fn from(c: CaseStatement) -> Self {
        Self::Case(c)
    }
}

impl From<IfStatement> for Statement {
    fn from(i: IfStatement) -> Self {
        Self::If(i)
    }
}

impl From<WhileStatement> for Statement {
    fn from(w: WhileStatement) -> Self {
        Self::While(w)
    }
}

impl From<RaiseStatement> for Statement {
    fn from(r: RaiseStatement) -> Self {
        Self::Raise(r)
    }
}

impl From<Function> for Statement {
    fn from(f: Function) -> Self {
        Self::Call(f)
    }
}

impl From<OpenStatement> for Statement {
    fn from(o: OpenStatement) -> Self {
        Self::Open(o)
    }
}

impl From<Delete> for Statement {
    fn from(d: Delete) -> Self {
        Self::Delete(d)
    }
}

impl From<CreateTable> for Statement {
    fn from(c: CreateTable) -> Self {
        Self::CreateTable(c)
    }
}

impl From<CreateIndex> for Statement {
    fn from(c: CreateIndex) -> Self {
        Self::CreateIndex(c)
    }
}

impl From<CreateServerStatement> for Statement {
    fn from(c: CreateServerStatement) -> Self {
        Self::CreateServer(c)
    }
}

impl From<CreateConnector> for Statement {
    fn from(c: CreateConnector) -> Self {
        Self::CreateConnector(c)
    }
}

impl From<AlterSchema> for Statement {
    fn from(a: AlterSchema) -> Self {
        Self::AlterSchema(a)
    }
}

impl From<AlterType> for Statement {
    fn from(a: AlterType) -> Self {
        Self::AlterType(a)
    }
}

impl From<DropDomain> for Statement {
    fn from(d: DropDomain) -> Self {
        Self::DropDomain(d)
    }
}

impl From<ShowCharset> for Statement {
    fn from(s: ShowCharset) -> Self {
        Self::ShowCharset(s)
    }
}

impl From<ShowObjects> for Statement {
    fn from(s: ShowObjects) -> Self {
        Self::ShowObjects(s)
    }
}

impl From<Use> for Statement {
    fn from(u: Use) -> Self {
        Self::Use(u)
    }
}

impl From<CreateFunction> for Statement {
    fn from(c: CreateFunction) -> Self {
        Self::CreateFunction(c)
    }
}

impl From<CreateTrigger> for Statement {
    fn from(c: CreateTrigger) -> Self {
        Self::CreateTrigger(c)
    }
}

impl From<DropTrigger> for Statement {
    fn from(d: DropTrigger) -> Self {
        Self::DropTrigger(d)
    }
}

impl From<DenyStatement> for Statement {
    fn from(d: DenyStatement) -> Self {
        Self::Deny(d)
    }
}

impl From<CreateDomain> for Statement {
    fn from(c: CreateDomain) -> Self {
        Self::CreateDomain(c)
    }
}

impl From<RenameTable> for Statement {
    fn from(r: RenameTable) -> Self {
        vec![r].into()
    }
}

impl From<Vec<RenameTable>> for Statement {
    fn from(r: Vec<RenameTable>) -> Self {
        Self::RenameTable(r)
    }
}

impl From<PrintStatement> for Statement {
    fn from(p: PrintStatement) -> Self {
        Self::Print(p)
    }
}

impl From<ReturnStatement> for Statement {
    fn from(r: ReturnStatement) -> Self {
        Self::Return(r)
    }
}

impl From<ExportData> for Statement {
    fn from(e: ExportData) -> Self {
        Self::ExportData(e)
    }
}

impl From<CreateUser> for Statement {
    fn from(c: CreateUser) -> Self {
        Self::CreateUser(c)
    }
}

impl From<VacuumStatement> for Statement {
    fn from(v: VacuumStatement) -> Self {
        Self::Vacuum(v)
    }
}
