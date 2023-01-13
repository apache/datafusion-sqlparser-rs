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
//! Test SQL syntax, which all main sqlparser dialects supported by sigma must parse
//! in the same way.

#[macro_use]
mod test_utils;
use test_utils::*;

use sqlparser::ast::BinaryOperator::*;
use sqlparser::ast::DataType::*;
use sqlparser::ast::Expr::*;
use sqlparser::ast::FunctionArg::*;
use sqlparser::ast::IsCheck::*;
use sqlparser::ast::JoinConstraint::*;
use sqlparser::ast::JoinOperator::*;
use sqlparser::ast::SelectItem::*;
use sqlparser::ast::SetExpr::*;
use sqlparser::ast::TableFactor::*;
use sqlparser::ast::Value::*;
use sqlparser::ast::WindowFrameBound::*;
use sqlparser::ast::WindowFrameUnits::*;
use sqlparser::ast::WindowSpec::*;
use sqlparser::ast::*;
use sqlparser::ast::{Function, Query, Select};

#[test]
fn parse_complicated_sql() {
    let ast = include_str!("queries/tpch/23.sql");
    let res = sigma_main_dialects().parse_sql_statements(ast);
    let actual_res = match res {
        Ok(e) => e,
        _ => Vec::new(),
    };
    let expected: Vec<Statement> = vec![sqlparser::ast::Statement::Query(Box::new(Query {
        with: Some(With {
            recursive: false,
            cte_tables: vec![
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "productivity".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: true,
                            top: None,
                            projection: vec![
                                UnnamedExpr(CompoundIdentifier(vec![
                                    Ident {
                                        value: "o".to_string(),
                                        quote_style: None,
                                    },
                                    Ident {
                                        value: "owner_id".to_string(),
                                        quote_style: None,
                                    },
                                ])),
                                UnnamedExpr(CompoundIdentifier(vec![
                                    Ident {
                                        value: "u".to_string(),
                                        quote_style: None,
                                    },
                                    Ident {
                                        value: "name".to_string(),
                                        quote_style: None,
                                    },
                                ])),
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "iff".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![
                                            Unnamed(BinaryOp {
                                                left: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "u".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "segment_c".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                                op: Eq,
                                                right: Box::new(Value(SingleQuotedString("Enterprise".to_string()))),
                                            }),
                                            Unnamed(Expr::Function(Function {
                                                name: ObjectName(vec![Ident {
                                                    value: "iff".to_string(),
                                                    quote_style: None,
                                                }]),
                                                args: vec![
                                                    Unnamed(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "u".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "geo_c".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: Eq,
                                                        right: Box::new(Value(SingleQuotedString("EMEA".to_string()))),
                                                    }),
                                                    Unnamed(Value(SingleQuotedString("EMEA".to_string()))),
                                                    Unnamed(Expr::Function(Function {
                                                        name: ObjectName(vec![Ident {
                                                            value: "iff".to_string(),
                                                            quote_style: None,
                                                        }]),
                                                        args: vec![
                                                            Unnamed(BinaryOp {
                                                                left: Box::new(CompoundIdentifier(vec![
                                                                    Ident {
                                                                        value: "u".to_string(),
                                                                        quote_style: None,
                                                                    },
                                                                    Ident {
                                                                        value: "geo_c".to_string(),
                                                                        quote_style: None,
                                                                    },
                                                                ])),
                                                                op: Eq,
                                                                right: Box::new(Value(SingleQuotedString(
                                                                    "APAC".to_string(),
                                                                ))),
                                                            }),
                                                            Unnamed(Value(SingleQuotedString(
                                                                "APAC".to_string(),
                                                            ))),
                                                            Unnamed(Value(SingleQuotedString(
                                                                "Enterprise".to_string(),
                                                            ))),
                                                        ],
                                                        within_group: vec![],
                                                        over: None,
                                                        distinct: false,
                                                        ignore_respect_nulls: None,
                                                        order_by: vec![],
                                                        limit: None,
                                                        outer_ignore_respect_nulls: None,
                                                    })),
                                                ],
                                                within_group: vec![],
                                                over: None,
                                                distinct: false,
                                                ignore_respect_nulls: None,
                                                order_by: vec![],
                                                limit: None,
                                                outer_ignore_respect_nulls: None,
                                            })),
                                            Unnamed(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "u".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "segment_c".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                        ],
                                        within_group: vec![],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "segment_c".to_string(),
                                        quote_style: None,
                                    },
                                },
                                UnnamedExpr(Identifier(Ident {
                                    value: "start_date_c".to_string(),
                                    quote_style: None,
                                })),
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "iff".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![
                                            Unnamed(Is {
                                                expr: Box::new(Identifier(Ident {
                                                    value: "termination_date_c".to_string(),
                                                    quote_style: None,
                                                })),
                                                check: NULL,
                                                negated: false,
                                            }),
                                            Unnamed(Expr::Function(Function {
                                                name: ObjectName(vec![Ident {
                                                    value: "date_trunc".to_string(),
                                                    quote_style: None,
                                                }]),
                                                args: vec![
                                                    Unnamed(Identifier(Ident {
                                                        value: "month".to_string(),
                                                        quote_style: None,
                                                    })),
                                                    Unnamed(Identifier(Ident {
                                                        value: "current_date".to_string(),
                                                        quote_style: None,
                                                    })),
                                                ],
                                                within_group: vec![],
                                                over: None,
                                                distinct: false,
                                                ignore_respect_nulls: None,
                                                order_by: vec![],
                                                limit: None,
                                                outer_ignore_respect_nulls: None,
                                            })),
                                            Unnamed(Expr::Function(Function {
                                                name: ObjectName(vec![Ident {
                                                    value: "dateadd".to_string(),
                                                    quote_style: None,
                                                }]),
                                                args: vec![
                                                    Unnamed(Identifier(Ident {
                                                        value: "month".to_string(),
                                                        quote_style: None,
                                                    })),
                                                    Unnamed(Value(number("1"))),
                                                    Unnamed(Expr::Function(Function {
                                                        name: ObjectName(vec![Ident {
                                                            value: "date_trunc".to_string(),
                                                            quote_style: None,
                                                        }]),
                                                        args: vec![
                                                            Unnamed(Identifier(Ident {
                                                                value: "month".to_string(),
                                                                quote_style: None,
                                                            })),
                                                            Unnamed(Identifier(Ident {
                                                                value: "termination_date_c".to_string(),
                                                                quote_style: None,
                                                            })),
                                                        ],
                                                        within_group: vec![],
                                                        over: None,
                                                        distinct: false,
                                                        ignore_respect_nulls: None,
                                                        order_by: vec![],
                                                        limit: None,
                                                        outer_ignore_respect_nulls: None,
                                                    })),
                                                ],
                                                within_group: vec![],
                                                over: None,
                                                distinct: false,
                                                ignore_respect_nulls: None,
                                                order_by: vec![],
                                                limit: None,
                                                outer_ignore_respect_nulls: None,
                                            })),
                                        ],
                                        within_group: vec![],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "last_month".to_string(),
                                        quote_style: None,
                                    },
                                },
                                ExprWithAlias {
                                    expr: Cast {
                                        try_cast: false,
                                        expr: Box::new(Expr::Function(Function {
                                            name: ObjectName(vec![Ident {
                                                value: "date_trunc".to_string(),
                                                quote_style: None,
                                            }]),
                                            args: vec![
                                                Unnamed(Identifier(Ident {
                                                    value: "month".to_string(),
                                                    quote_style: None,
                                                })),
                                                Unnamed(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "o".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "close_date".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                            ],
                                            within_group: vec![],
                                            over: None,
                                            distinct: false,
                                            ignore_respect_nulls: None,
                                            order_by: vec![],
                                            limit: None,
                                            outer_ignore_respect_nulls: None,
                                        })),
                                        data_type: Date,
                                    },
                                    alias: Ident {
                                        value: "cohort_month".to_string(),
                                        quote_style: None,
                                    },
                                },
                                ExprWithAlias {
                                    expr: Case {
                                        operand: None,
                                        conditions: vec![
                                            BinaryOp {
                                                left: Box::new(Identifier(Ident {
                                                    value: "SEGMENT_C".to_string(),
                                                    quote_style: None,
                                                })),
                                                op: Eq,
                                                right: Box::new(Value(SingleQuotedString("Corporate".to_string()))),
                                            },
                                            BinaryOp {
                                                left: Box::new(BinaryOp {
                                                    left: Box::new(CompoundIdentifier(vec![
                                                        Ident {
                                                            value: "u".to_string(),
                                                            quote_style: None,
                                                        },
                                                        Ident {
                                                            value: "region_c".to_string(),
                                                            quote_style: None,
                                                        },
                                                    ])),
                                                    op: Eq,
                                                    right: Box::new(Value(SingleQuotedString("Northeast".to_string()))),
                                                }),
                                                op: Or,
                                                right: Box::new(BinaryOp {
                                                    left: Box::new(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "u".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "region_c".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: Eq,
                                                        right: Box::new(Value(SingleQuotedString(
                                                            "NY Metro".to_string(),
                                                        ))),
                                                    }),
                                                    op: And,
                                                    right: Box::new(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "u".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "segment_c".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: NotEq,
                                                        right: Box::new(Value(SingleQuotedString(
                                                            "Corporate".to_string(),
                                                        ))),
                                                    }),
                                                }),
                                            },
                                            BinaryOp {
                                                left: Box::new(BinaryOp {
                                                    left: Box::new(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "u".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "region_c".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: Eq,
                                                        right: Box::new(Value(SingleQuotedString("West".to_string()))),
                                                    }),
                                                    op: Or,
                                                    right: Box::new(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "u".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "region_c".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: Eq,
                                                        right: Box::new(Value(SingleQuotedString(
                                                            "Northern California".to_string(),
                                                        ))),
                                                    }),
                                                }),
                                                op: Or,
                                                right: Box::new(BinaryOp {
                                                    left: Box::new(CompoundIdentifier(vec![
                                                        Ident {
                                                            value: "u".to_string(),
                                                            quote_style: None,
                                                        },
                                                        Ident {
                                                            value: "region_c".to_string(),
                                                            quote_style: None,
                                                        },
                                                    ])),
                                                    op: Eq,
                                                    right: Box::new(Value(SingleQuotedString("PNW".to_string()))),
                                                }),
                                            },
                                            BinaryOp {
                                                left: Box::new(BinaryOp {
                                                    left: Box::new(BinaryOp {
                                                        left: Box::new(BinaryOp {
                                                            left: Box::new(CompoundIdentifier(vec![
                                                                Ident {
                                                                    value: "u".to_string(),
                                                                    quote_style: None,
                                                                },
                                                                Ident {
                                                                    value: "region_c".to_string(),
                                                                    quote_style: None,
                                                                },
                                                            ])),
                                                            op: Eq,
                                                            right: Box::new(Value(SingleQuotedString(
                                                                "Central".to_string(),
                                                            ))),
                                                        }),
                                                        op: Or,
                                                        right: Box::new(BinaryOp {
                                                            left: Box::new(CompoundIdentifier(vec![
                                                                Ident {
                                                                    value: "u".to_string(),
                                                                    quote_style: None,
                                                                },
                                                                Ident {
                                                                    value: "region_c".to_string(),
                                                                    quote_style: None,
                                                                },
                                                            ])),
                                                            op: Eq,
                                                            right: Box::new(Value(SingleQuotedString(
                                                                "North Central".to_string(),
                                                            ))),
                                                        }),
                                                    }),
                                                    op: Or,
                                                    right: Box::new(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "u".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "region_c".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: Eq,
                                                        right: Box::new(Value(SingleQuotedString("TOLA".to_string()))),
                                                    }),
                                                }),
                                                op: Or,
                                                right: Box::new(BinaryOp {
                                                    left: Box::new(CompoundIdentifier(vec![
                                                        Ident {
                                                            value: "u".to_string(),
                                                            quote_style: None,
                                                        },
                                                        Ident {
                                                            value: "region_c".to_string(),
                                                            quote_style: None,
                                                        },
                                                    ])),
                                                    op: Eq,
                                                    right: Box::new(Value(SingleQuotedString("Midwest".to_string()))),
                                                }),
                                            },
                                            BinaryOp {
                                                left: Box::new(BinaryOp {
                                                    left: Box::new(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "u".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "region_c".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: Eq,
                                                        right: Box::new(Value(SingleQuotedString(
                                                            "Southwest".to_string(),
                                                        ))),
                                                    }),
                                                    op: Or,
                                                    right: Box::new(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "u".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "region_c".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: Eq,
                                                        right: Box::new(Value(SingleQuotedString(
                                                            "NY Rockies".to_string(),
                                                        ))),
                                                    }),
                                                }),
                                                op: Or,
                                                right: Box::new(BinaryOp {
                                                    left: Box::new(CompoundIdentifier(vec![
                                                        Ident {
                                                            value: "u".to_string(),
                                                            quote_style: None,
                                                        },
                                                        Ident {
                                                            value: "region_c".to_string(),
                                                            quote_style: None,
                                                        },
                                                    ])),
                                                    op: Eq,
                                                    right: Box::new(Value(SingleQuotedString(
                                                        "Southern California".to_string(),
                                                    ))),
                                                }),
                                            },
                                            BinaryOp {
                                                left: Box::new(BinaryOp {
                                                    left: Box::new(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "u".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "region_c".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: Eq,
                                                        right: Box::new(Value(SingleQuotedString(
                                                            "Southeast".to_string(),
                                                        ))),
                                                    }),
                                                    op: Or,
                                                    right: Box::new(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "u".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "region_c".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: Eq,
                                                        right: Box::new(Value(SingleQuotedString(
                                                            "Philly Metro".to_string(),
                                                        ))),
                                                    }),
                                                }),
                                                op: Or,
                                                right: Box::new(BinaryOp {
                                                    left: Box::new(CompoundIdentifier(vec![
                                                        Ident {
                                                            value: "u".to_string(),
                                                            quote_style: None,
                                                        },
                                                        Ident {
                                                            value: "region_c".to_string(),
                                                            quote_style: None,
                                                        },
                                                    ])),
                                                    op: Eq,
                                                    right: Box::new(Value(SingleQuotedString("DMV".to_string()))),
                                                }),
                                            },
                                            BinaryOp {
                                                left: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "u".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "geo_c".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                                op: Eq,
                                                right: Box::new(Value(SingleQuotedString("EMEA".to_string()))),
                                            },
                                            BinaryOp {
                                                left: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "u".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "segment_c".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                                op: Eq,
                                                right: Box::new(Value(SingleQuotedString("APAC".to_string()))),
                                            },
                                            BinaryOp {
                                                left: Box::new(Identifier(Ident {
                                                    value: "segment_c".to_string(),
                                                    quote_style: None,
                                                })),
                                                op: Eq,
                                                right: Box::new(Value(SingleQuotedString("Majors".to_string()))),
                                            },
                                        ],
                                        results: vec![
                                            Value(SingleQuotedString("None".to_string())),
                                            Value(SingleQuotedString("Northeast".to_string())),
                                            Value(SingleQuotedString("West".to_string())),
                                            Value(SingleQuotedString("Central".to_string())),
                                            Value(SingleQuotedString("Southwest".to_string())),
                                            Value(SingleQuotedString("Southeast".to_string())),
                                            CompoundIdentifier(vec![
                                                Ident {
                                                    value: "u".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "region_c".to_string(),
                                                    quote_style: None,
                                                },
                                            ]),
                                            CompoundIdentifier(vec![
                                                Ident {
                                                    value: "u".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "segment_c".to_string(),
                                                    quote_style: None,
                                                },
                                            ]),
                                            CompoundIdentifier(vec![
                                                Ident {
                                                    value: "u".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "region_c".to_string(),
                                                    quote_style: None,
                                                },
                                            ]),
                                        ],
                                        else_result: Some(Box::new(Value(SingleQuotedString("None".to_string())))),
                                    },
                                    alias: Ident {
                                        value: "region".to_string(),
                                        quote_style: None,
                                    },
                                },
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "sum".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![Unnamed(Expr::Function(Function {
                                            name: ObjectName(vec![Ident {
                                                value: "iff".to_string(),
                                                quote_style: None,
                                            }]),
                                            args: vec![
                                                Unnamed(Is {
                                                    expr: Box::new(Identifier(Ident {
                                                        value: "forecast_acv_c".to_string(),
                                                        quote_style: None,
                                                    })),
                                                    check: NULL,
                                                    negated: true,
                                                }),
                                                Unnamed(Expr::Function(Function {
                                                    name: ObjectName(vec![Ident {
                                                        value: "iff".to_string(),
                                                        quote_style: None,
                                                    }]),
                                                    args: vec![
                                                        Unnamed(Is {
                                                            expr: Box::new(Identifier(Ident {
                                                                value: "base_renewal_acv_c".to_string(),
                                                                quote_style: None,
                                                            })),
                                                            check: NULL,
                                                            negated: false,
                                                        }),
                                                        Unnamed(Expr::Function(Function {
                                                            name: ObjectName(vec![Ident {
                                                                value: "iff".to_string(),
                                                                quote_style: None,
                                                            }]),
                                                            args: vec![
                                                                Unnamed(BinaryOp {
                                                                    left: Box::new(BinaryOp {
                                                                        left: Box::new(Identifier(Ident {
                                                                            value: "forecast_acv_c".to_string(),
                                                                            quote_style: None,
                                                                        })),
                                                                        op: Minus,
                                                                        right: Box::new(Value(number("0"))),
                                                                    }),
                                                                    op: Lt,
                                                                    right: Box::new(Value(number("1"))),
                                                                }),
                                                                Unnamed(Value(number("0"))),
                                                                Unnamed(BinaryOp {
                                                                    left: Box::new(Identifier(Ident {
                                                                        value: "forecast_acv_c".to_string(),
                                                                        quote_style: None,
                                                                    })),
                                                                    op: Minus,
                                                                    right: Box::new(Value(number("0"))),
                                                                }),
                                                            ],
                                                            within_group: vec![],
                                                            over: None,
                                                            distinct: false,
                                                            ignore_respect_nulls: None,
                                                            order_by: vec![],
                                                            limit: None,
                                                            outer_ignore_respect_nulls: None,
                                                        })),
                                                        Unnamed(Expr::Function(Function {
                                                            name: ObjectName(vec![Ident {
                                                                value: "iff".to_string(),
                                                                quote_style: None,
                                                            }]),
                                                            args: vec![
                                                                Unnamed(BinaryOp {
                                                                    left: Box::new(BinaryOp {
                                                                        left: Box::new(Identifier(Ident {
                                                                            value: "forecast_acv_c".to_string(),
                                                                            quote_style: None,
                                                                        })),
                                                                        op: Minus,
                                                                        right: Box::new(Identifier(Ident {
                                                                            value:
                                                                                "base_renewal_acv_c".to_string(),
                                                                            quote_style: None,
                                                                        })),
                                                                    }),
                                                                    op: Lt,
                                                                    right: Box::new(Value(number("1"))),
                                                                }),
                                                                Unnamed(Value(number("0"))),
                                                                Unnamed(BinaryOp {
                                                                    left: Box::new(Identifier(Ident {
                                                                        value: "forecast_acv_c".to_string(),
                                                                        quote_style: None,
                                                                    })),
                                                                    op: Minus,
                                                                    right: Box::new(Identifier(Ident {
                                                                        value: "base_renewal_acv_c".to_string(),
                                                                        quote_style: None,
                                                                    })),
                                                                }),
                                                            ],
                                                            within_group: vec![],
                                                            over: None,
                                                            distinct: false,
                                                            ignore_respect_nulls: None,
                                                            order_by: vec![],
                                                            limit: None,
                                                            outer_ignore_respect_nulls: None,
                                                        })),
                                                    ],
                                                    within_group: vec![],
                                                    over: None,
                                                    distinct: false,
                                                    ignore_respect_nulls: None,
                                                    order_by: vec![],
                                                    limit: None,
                                                    outer_ignore_respect_nulls: None,
                                                })),
                                                Unnamed(Value(number("0"))),
                                            ],
                                            within_group: vec![],
                                            over: None,
                                            distinct: false,
                                            ignore_respect_nulls: None,
                                            order_by: vec![],
                                            limit: None,
                                            outer_ignore_respect_nulls: None,
                                        }))],
                                        within_group: vec![],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "bookings".to_string(),
                                        quote_style: None,
                                    },
                                },
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![
                                        Ident {
                                            value: "fivetran".to_string(),
                                            quote_style: None,
                                        },
                                        Ident {
                                            value: "salesforce".to_string(),
                                            quote_style: None,
                                        },
                                        Ident {
                                            value: "opportunity".to_string(),
                                            quote_style: None,
                                        },
                                    ]),
                                    alias: Some(TableAlias {
                                        name: Ident {
                                            value: "o".to_string(),
                                            quote_style: None,
                                        },
                                        columns: vec![],
                                    }),
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![
                                    Join {
                                        relation: Table {
                                            name: ObjectName(vec![
                                                Ident {
                                                    value: "fivetran".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "salesforce".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "user".to_string(),
                                                    quote_style: None,
                                                },
                                            ]),
                                            alias: Some(TableAlias {
                                                name: Ident {
                                                    value: "u".to_string(),
                                                    quote_style: None,
                                                },
                                                columns: vec![],
                                            }),
                                            args: vec![],
                                            with_hints: vec![],
                                        },
                                        join_operator: LeftOuter(On(BinaryOp {
                                            left: Box::new(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "u".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "id".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                            op: Eq,
                                            right: Box::new(Identifier(Ident {
                                                value: "owner_id".to_string(),
                                                quote_style: None,
                                            })),
                                        })),
                                    },
                                    Join {
                                        relation: Table {
                                            name: ObjectName(vec![
                                                Ident {
                                                    value: "fivetran".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "salesforce".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "account".to_string(),
                                                    quote_style: None,
                                                },
                                            ]),
                                            alias: Some(TableAlias {
                                                name: Ident {
                                                    value: "a".to_string(),
                                                    quote_style: None,
                                                },
                                                columns: vec![],
                                            }),
                                            args: vec![],
                                            with_hints: vec![],
                                        },
                                        join_operator: LeftOuter(On(BinaryOp {
                                            left: Box::new(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "a".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "id".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                            op: Eq,
                                            right: Box::new(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "o".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "account_id".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                        })),
                                    },
                                ],
                            }],
                            selection: Some(BinaryOp {
                                left: Box::new(BinaryOp {
                                    left: Box::new(BinaryOp {
                                        left: Box::new(BinaryOp {
                                            left: Box::new(BinaryOp {
                                                left: Box::new(Identifier(Ident {
                                                    value: "stage_name".to_string(),
                                                    quote_style: None,
                                                })),
                                                op: Eq,
                                                right: Box::new(Value(SingleQuotedString("Closed Won".to_string()))),
                                            }),
                                            op: And,
                                            right: Box::new(BinaryOp {
                                                left: Box::new(Identifier(Ident {
                                                    value: "close_date".to_string(),
                                                    quote_style: None,
                                                })),
                                                op: GtEq,
                                                right: Box::new(Value(SingleQuotedString("2015-02-01".to_string()))),
                                            }),
                                        }),
                                        op: And,
                                        right: Box::new(BinaryOp {
                                            left: Box::new(Identifier(Ident {
                                                value: "start_date_c".to_string(),
                                                quote_style: None,
                                            })),
                                            op: Lt,
                                            right: Box::new(Identifier(Ident {
                                                value: "cohort_month".to_string(),
                                                quote_style: None,
                                            })),
                                        }),
                                    }),
                                    op: And,
                                    right: Box::new(BinaryOp {
                                        left: Box::new(CompoundIdentifier(vec![
                                            Ident {
                                                value: "u".to_string(),
                                                quote_style: None,
                                            },
                                            Ident {
                                                value: "function_c".to_string(),
                                                quote_style: None,
                                            },
                                        ])),
                                        op: Eq,
                                        right: Box::new(Value(SingleQuotedString("Account Executive".to_string()))),
                                    }),
                                }),
                                op: And,
                                right: Box::new(BinaryOp {
                                    left: Box::new(Identifier(Ident {
                                        value: "start_date_c".to_string(),
                                        quote_style: None,
                                    })),
                                    op: GtEq,
                                    right: Box::new(Value(SingleQuotedString("2015-02-01".to_string()))),
                                }),
                            }),
                            group_by: vec![
                                Value(number("1")),
                                Value(number("2")),
                                Value(number("3")),
                                Value(number("4")),
                                Value(number("5")),
                                Value(number("6")),
                                Value(number("7")),
                            ],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![OrderByExpr {
                            expr: Identifier(Ident {
                                value: "cohort_month".to_string(),
                                quote_style: None,
                            }),
                            asc: Some(true),
                            nulls_first: None,
                        }],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "missing_months".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: true,
                            top: None,
                            projection: vec![
                                ExprWithAlias {
                                    expr: Cast {
                                        try_cast: false,
                                        expr: Box::new(Expr::Function(Function {
                                            name: ObjectName(vec![Ident {
                                                value: "date_trunc".to_string(),
                                                quote_style: None,
                                            }]),
                                            args: vec![
                                                Unnamed(Identifier(Ident {
                                                    value: "month".to_string(),
                                                    quote_style: None,
                                                })),
                                                Unnamed(Identifier(Ident {
                                                    value: "_date".to_string(),
                                                    quote_style: None,
                                                })),
                                            ],
                                            within_group: vec![],
                                            over: None,
                                            distinct: false,
                                            ignore_respect_nulls: None,
                                            order_by: vec![],
                                            limit: None,
                                            outer_ignore_respect_nulls: None,
                                        })),
                                        data_type: Date,
                                    },
                                    alias: Ident {
                                        value: "cohort_month".to_string(),
                                        quote_style: None,
                                    },
                                },
                                UnnamedExpr(Identifier(Ident {
                                    value: "owner_id".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "name".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(CompoundIdentifier(vec![
                                    Ident {
                                        value: "p".to_string(),
                                        quote_style: None,
                                    },
                                    Ident {
                                        value: "segment_c".to_string(),
                                        quote_style: None,
                                    },
                                ])),
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "iff".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![
                                            Unnamed(Is {
                                                expr: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "p".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "region".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                                check: NULL,
                                                negated: false,
                                            }),
                                            Unnamed(Value(SingleQuotedString("None".to_string()))),
                                            Unnamed(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "p".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "region".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                        ],
                                        within_group: vec![],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "region".to_string(),
                                        quote_style: None,
                                    },
                                },
                                ExprWithAlias {
                                    expr: CompoundIdentifier(vec![
                                        Ident {
                                            value: "p".to_string(),
                                            quote_style: None,
                                        },
                                        Ident {
                                            value: "start_date_c".to_string(),
                                            quote_style: None,
                                        },
                                    ]),
                                    alias: Ident {
                                        value: "sd".to_string(),
                                        quote_style: None,
                                    },
                                },
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![
                                        Ident {
                                            value: "snowhouse".to_string(),
                                            quote_style: None,
                                        },
                                        Ident {
                                            value: "utils".to_string(),
                                            quote_style: None,
                                        },
                                        Ident {
                                            value: "calendar".to_string(),
                                            quote_style: None,
                                        },
                                    ]),
                                    alias: Some(TableAlias {
                                        name: Ident {
                                            value: "c".to_string(),
                                            quote_style: None,
                                        },
                                        columns: vec![],
                                    }),
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![Join {
                                    relation: Table {
                                        name: ObjectName(vec![Ident {
                                            value: "productivity".to_string(),
                                            quote_style: None,
                                        }]),
                                        alias: Some(TableAlias {
                                            name: Ident {
                                                value: "p".to_string(),
                                                quote_style: None,
                                            },
                                            columns: vec![],
                                        }),
                                        args: vec![],
                                        with_hints: vec![],
                                    },
                                    join_operator: Inner(On(Between {
                                        expr: Box::new(Cast {
                                            try_cast: false,
                                            expr: Box::new(Expr::Function(Function {
                                                name: ObjectName(vec![Ident {
                                                    value: "date_trunc".to_string(),
                                                    quote_style: None,
                                                }]),
                                                args: vec![
                                                    Unnamed(Identifier(Ident {
                                                        value: "month".to_string(),
                                                        quote_style: None,
                                                    })),
                                                    Unnamed(Identifier(Ident {
                                                        value: "_date".to_string(),
                                                        quote_style: None,
                                                    })),
                                                ],
                                                within_group: vec![],
                                                over: None,
                                                distinct: false,
                                                ignore_respect_nulls: None,
                                                order_by: vec![],
                                                limit: None,
                                                outer_ignore_respect_nulls: None,
                                            })),
                                            data_type: Date,
                                        }),
                                        negated: false,
                                        low: Box::new(Expr::Function(Function {
                                            name: ObjectName(vec![Ident {
                                                value: "date_trunc".to_string(),
                                                quote_style: None,
                                            }]),
                                            args: vec![
                                                Unnamed(Identifier(Ident {
                                                    value: "month".to_string(),
                                                    quote_style: None,
                                                })),
                                                Unnamed(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "p".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "start_date_c".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                            ],
                                            within_group: vec![],
                                            over: None,
                                            distinct: false,
                                            ignore_respect_nulls: None,
                                            order_by: vec![],
                                            limit: None,
                                            outer_ignore_respect_nulls: None,
                                        })),
                                        high: Box::new(Expr::Function(Function {
                                            name: ObjectName(vec![Ident {
                                                value: "coalesce".to_string(),
                                                quote_style: None,
                                            }]),
                                            args: vec![
                                                Unnamed(Expr::Function(Function {
                                                    name: ObjectName(vec![Ident {
                                                        value: "DATEADD".to_string(),
                                                        quote_style: None,
                                                    }]),
                                                    args: vec![
                                                        Unnamed(Identifier(Ident {
                                                            value: "month".to_string(),
                                                            quote_style: None,
                                                        })),
                                                        Unnamed(Value(number("0"))),
                                                        Unnamed(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "p".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "last_month".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                    ],
                                                    within_group: vec![],
                                                    over: None,
                                                    distinct: false,
                                                    ignore_respect_nulls: None,
                                                    order_by: vec![],
                                                    limit: None,
                                                    outer_ignore_respect_nulls: None,
                                                })),
                                                Unnamed(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "p".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "start_date_c".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                            ],
                                            within_group: vec![],
                                            over: None,
                                            distinct: false,
                                            ignore_respect_nulls: None,
                                            order_by: vec![],
                                            limit: None,
                                            outer_ignore_respect_nulls: None,
                                        })),
                                    })),
                                }],
                            }],
                            selection: Some(BinaryOp {
                                left: Box::new(BinaryOp {
                                    left: Box::new(BinaryOp {
                                        left: Box::new(Identifier(Ident {
                                            value: "_date".to_string(),
                                            quote_style: None,
                                        })),
                                        op: Gt,
                                        right: Box::new(Value(SingleQuotedString("2015-02-01".to_string()))),
                                    }),
                                    op: And,
                                    right: Box::new(BinaryOp {
                                        left: Box::new(Identifier(Ident {
                                            value: "_date".to_string(),
                                            quote_style: None,
                                        })),
                                        op: LtEq,
                                        right: Box::new(Identifier(Ident {
                                            value: "current_date".to_string(),
                                            quote_style: None,
                                        })),
                                    }),
                                }),
                                op: And,
                                right: Box::new(BinaryOp {
                                    left: Box::new(Identifier(Ident {
                                        value: "last_month".to_string(),
                                        quote_style: None,
                                    })),
                                    op: GtEq,
                                    right: Box::new(Identifier(Ident {
                                        value: "cohort_month".to_string(),
                                        quote_style: None,
                                    })),
                                }),
                            }),
                            group_by: vec![],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![
                            OrderByExpr {
                                expr: Identifier(Ident {
                                    value: "owner_id".to_string(),
                                    quote_style: None,
                                }),
                                asc: Some(false),
                                nulls_first: None,
                            },
                            OrderByExpr {
                                expr: Identifier(Ident {
                                    value: "cohort_month".to_string(),
                                    quote_style: None,
                                }),
                                asc: Some(true),
                                nulls_first: None,
                            },
                        ],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "reps_padded_with_month".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![
                                SelectItem::Wildcard {
                                    prefix: Some(ObjectName(vec![Ident {
                                        value: "m".to_string(),
                                        quote_style: None,
                                    }])),
                                    except: vec![],
                                    replace: vec![],
                                },
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "iff".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![
                                            Unnamed(Is {
                                                expr: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "p".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "bookings".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                                check: NULL,
                                                negated: false,
                                            }),
                                            Unnamed(Value(number("0"))),
                                            Unnamed(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "p".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "bookings".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                        ],
                                        within_group: vec![],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "bookings".to_string(),
                                        quote_style: None,
                                    },
                                },
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![Ident {
                                        value: "missing_months".to_string(),
                                        quote_style: None,
                                    }]),
                                    alias: Some(TableAlias {
                                        name: Ident {
                                            value: "m".to_string(),
                                            quote_style: None,
                                        },
                                        columns: vec![],
                                    }),
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![Join {
                                    relation: Table {
                                        name: ObjectName(vec![Ident {
                                            value: "productivity".to_string(),
                                            quote_style: None,
                                        }]),
                                        alias: Some(TableAlias {
                                            name: Ident {
                                                value: "p".to_string(),
                                                quote_style: None,
                                            },
                                            columns: vec![],
                                        }),
                                        args: vec![],
                                        with_hints: vec![],
                                    },
                                    join_operator: LeftOuter(On(BinaryOp {
                                        left: Box::new(BinaryOp {
                                            left: Box::new(BinaryOp {
                                                left: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "p".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "owner_id".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                                op: Eq,
                                                right: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "m".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "owner_id".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                            }),
                                            op: And,
                                            right: Box::new(BinaryOp {
                                                left: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "m".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "cohort_month".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                                op: Eq,
                                                right: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "p".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "cohort_month".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                            }),
                                        }),
                                        op: And,
                                        right: Box::new(BinaryOp {
                                            left: Box::new(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "m".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "region".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                            op: Eq,
                                            right: Box::new(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "p".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "region".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                        }),
                                    })),
                                }],
                            }],
                            selection: None,
                            group_by: vec![],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "pre_pivot_work".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "row_number".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![],
                                        within_group: vec![],
                                        over: Some(Inline(InlineWindowSpec {
                                            partition_by: vec![Identifier(Ident {
                                                value: "owner_id".to_string(),
                                                quote_style: None,
                                            })],
                                            order_by: vec![OrderByExpr {
                                                expr: Identifier(Ident {
                                                    value: "cohort_month".to_string(),
                                                    quote_style: None,
                                                }),
                                                asc: Some(true),
                                                nulls_first: None,
                                            }],
                                            window_frame: None,
                                        })),
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "active_month".to_string(),
                                        quote_style: None,
                                    },
                                },
                                UnnamedExpr(Identifier(Ident {
                                    value: "owner_id".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "name".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "region".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "segment_c".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "bookings".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "sd".to_string(),
                                    quote_style: None,
                                })),
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![Ident {
                                        value: "reps_padded_with_month".to_string(),
                                        quote_style: None,
                                    }]),
                                    alias: None,
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![],
                            }],
                            selection: None,
                            group_by: vec![],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "rolling_sum".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![
                                UnnamedExpr(Identifier(Ident {
                                    value: "owner_id".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "name".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "region".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "segment_c".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "active_month".to_string(),
                                    quote_style: None,
                                })),
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "last_value".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![Unnamed(Identifier(Ident {
                                            value: "active_month".to_string(),
                                            quote_style: None,
                                        }))],
                                        within_group: vec![],
                                        over: Some(Inline(InlineWindowSpec {
                                            partition_by: vec![Identifier(Ident {
                                                value: "owner_id".to_string(),
                                                quote_style: None,
                                            })],
                                            order_by: vec![OrderByExpr {
                                                expr: Identifier(Ident {
                                                    value: "active_month".to_string(),
                                                    quote_style: None,
                                                }),
                                                asc: Some(true),
                                                nulls_first: None,
                                            }],
                                            window_frame: None,
                                        })),
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "tenure".to_string(),
                                        quote_style: None,
                                    },
                                },
                                UnnamedExpr(Identifier(Ident {
                                    value: "sd".to_string(),
                                    quote_style: None,
                                })),
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "sum".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![Unnamed(Identifier(Ident {
                                            value: "bookings".to_string(),
                                            quote_style: None,
                                        }))],
                                        within_group: vec![],
                                        over: Some(Inline(InlineWindowSpec {
                                            partition_by: vec![Identifier(Ident {
                                                value: "owner_id".to_string(),
                                                quote_style: None,
                                            })],
                                            order_by: vec![OrderByExpr {
                                                expr: Identifier(Ident {
                                                    value: "active_month".to_string(),
                                                    quote_style: None,
                                                }),
                                                asc: Some(true),
                                                nulls_first: None,
                                            }],
                                            window_frame: None,
                                        })),
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "p".to_string(),
                                        quote_style: None,
                                    },
                                },
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![Ident {
                                        value: "pre_pivot_work".to_string(),
                                        quote_style: None,
                                    }]),
                                    alias: None,
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![],
                            }],
                            selection: None,
                            group_by: vec![],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "ltm".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![
                                UnnamedExpr(Identifier(Ident {
                                    value: "owner_id".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "name".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "region".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "segment_c".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "active_month".to_string(),
                                    quote_style: None,
                                })),
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "last_value".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![Unnamed(Identifier(Ident {
                                            value: "active_month".to_string(),
                                            quote_style: None,
                                        }))],
                                        within_group: vec![],
                                        over: Some(Inline(InlineWindowSpec {
                                            partition_by: vec![Identifier(Ident {
                                                value: "owner_id".to_string(),
                                                quote_style: None,
                                            })],
                                            order_by: vec![OrderByExpr {
                                                expr: Identifier(Ident {
                                                    value: "active_month".to_string(),
                                                    quote_style: None,
                                                }),
                                                asc: Some(true),
                                                nulls_first: None,
                                            }],
                                            window_frame: None,
                                        })),
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "tenure".to_string(),
                                        quote_style: None,
                                    },
                                },
                                UnnamedExpr(Identifier(Ident {
                                    value: "sd".to_string(),
                                    quote_style: None,
                                })),
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "iff".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![
                                            Unnamed(BinaryOp {
                                                left: Box::new(Identifier(Ident {
                                                    value: "active_month".to_string(),
                                                    quote_style: None,
                                                })),
                                                op: GtEq,
                                                right: Box::new(Value(number("12"))),
                                            }),
                                            Unnamed(Expr::Function(Function {
                                                name: ObjectName(vec![Ident {
                                                    value: "sum".to_string(),
                                                    quote_style: None,
                                                }]),
                                                args: vec![Unnamed(Identifier(Ident {
                                                    value: "bookings".to_string(),
                                                    quote_style: None,
                                                }))],
                                                within_group: vec![],
                                                over: Some(Inline(InlineWindowSpec {
                                                    partition_by: vec![Identifier(Ident {
                                                        value: "owner_id".to_string(),
                                                        quote_style: None,
                                                    })],
                                                    order_by: vec![OrderByExpr {
                                                        expr: Identifier(Ident {
                                                            value: "active_month".to_string(),
                                                            quote_style: None,
                                                        }),
                                                        asc: Some(true),
                                                        nulls_first: None,
                                                    }],
                                                    window_frame: Some(WindowFrame {
                                                        units: Rows,
                                                        start_bound: Preceding(Some(11)),
                                                        end_bound: None,
                                                    }),
                                                })),
                                                distinct: false,
                                                ignore_respect_nulls: None,
                                                order_by: vec![],
                                                limit: None,
                                                outer_ignore_respect_nulls: None,
                                            })),
                                            Unnamed(Expr::Function(Function {
                                                name: ObjectName(vec![Ident {
                                                    value: "sum".to_string(),
                                                    quote_style: None,
                                                }]),
                                                args: vec![Unnamed(Identifier(Ident {
                                                    value: "bookings".to_string(),
                                                    quote_style: None,
                                                }))],
                                                within_group: vec![],
                                                over: Some(Inline(InlineWindowSpec {
                                                    partition_by: vec![Identifier(Ident {
                                                        value: "owner_id".to_string(),
                                                        quote_style: None,
                                                    })],
                                                    order_by: vec![OrderByExpr {
                                                        expr: Identifier(Ident {
                                                            value: "active_month".to_string(),
                                                            quote_style: None,
                                                        }),
                                                        asc: None,
                                                        nulls_first: None,
                                                    }],
                                                    window_frame: None,
                                                })),
                                                distinct: false,
                                                ignore_respect_nulls: None,
                                                order_by: vec![],
                                                limit: None,
                                                outer_ignore_respect_nulls: None,
                                            })),
                                        ],
                                        within_group: vec![],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "p".to_string(),
                                        quote_style: None,
                                    },
                                },
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![Ident {
                                        value: "pre_pivot_work".to_string(),
                                        quote_style: None,
                                    }]),
                                    alias: None,
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![],
                            }],
                            selection: None,
                            group_by: vec![],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "years_included".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![
                                SelectItem::Wildcard {
                                    prefix: None,
                                    except: vec![],
                                    replace: vec![],
                                },
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "date_trunc".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![
                                            Unnamed(Identifier(Ident {
                                                value: "year".to_string(),
                                                quote_style: None,
                                            })),
                                            Unnamed(Identifier(Ident {
                                                value: "sd".to_string(),
                                                quote_style: None,
                                            })),
                                        ],
                                        within_group: vec![],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "start_year".to_string(),
                                        quote_style: None,
                                    },
                                },
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![Ident {
                                        value: "ltm".to_string(),
                                        quote_style: None,
                                    }]),
                                    alias: None,
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![],
                            }],
                            selection: None,
                            group_by: vec![],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "all_reps".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![
                                SelectItem::Wildcard {
                                    prefix: Some(ObjectName(vec![Ident {
                                        value: "a".to_string(),
                                        quote_style: None,
                                    }])),
                                    except: vec![],
                                    replace: vec![],
                                },
                                ExprWithAlias {
                                    expr: CompoundIdentifier(vec![
                                        Ident {
                                            value: "a".to_string(),
                                            quote_style: None,
                                        },
                                        Ident {
                                            value: "p".to_string(),
                                            quote_style: None,
                                        },
                                    ]),
                                    alias: Ident {
                                        value: "growth_bookings".to_string(),
                                        quote_style: None,
                                    },
                                },
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "max".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![Unnamed(CompoundIdentifier(vec![
                                            Ident {
                                                value: "a".to_string(),
                                                quote_style: None,
                                            },
                                            Ident {
                                                value: "p".to_string(),
                                                quote_style: None,
                                            },
                                        ]))],
                                        within_group: vec![],
                                        over: Some(Inline(InlineWindowSpec {
                                            partition_by: vec![CompoundIdentifier(vec![
                                                Ident {
                                                    value: "a".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "name".to_string(),
                                                    quote_style: None,
                                                },
                                            ])],
                                            order_by: vec![],
                                            window_frame: None,
                                        })),
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "max_growth".to_string(),
                                        quote_style: None,
                                    },
                                },
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "iff".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![
                                            Unnamed(BinaryOp {
                                                left: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "a".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "p".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                                op: LtEq,
                                                right: Box::new(Value(number("0"))),
                                            }),
                                            Unnamed(BinaryOp {
                                                left: Box::new(Nested(vec![BinaryOp {
                                                    left: Box::new(CompoundIdentifier(vec![
                                                        Ident {
                                                            value: "a".to_string(),
                                                            quote_style: None,
                                                        },
                                                        Ident {
                                                            value: "p".to_string(),
                                                            quote_style: None,
                                                        },
                                                    ])),
                                                    op: Minus,
                                                    right: Box::new(CompoundIdentifier(vec![
                                                        Ident {
                                                            value: "b".to_string(),
                                                            quote_style: None,
                                                        },
                                                        Ident {
                                                            value: "p".to_string(),
                                                            quote_style: None,
                                                        },
                                                    ])),
                                                }])),
                                                op: Divide,
                                                right: Box::new(Value(number("1"))),
                                            }),
                                            Unnamed(Expr::Function(Function {
                                                name: ObjectName(vec![Ident {
                                                    value: "iff".to_string(),
                                                    quote_style: None,
                                                }]),
                                                args: vec![
                                                    Unnamed(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "b".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "p".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: LtEq,
                                                        right: Box::new(Value(number("0"))),
                                                    }),
                                                    Unnamed(BinaryOp {
                                                        left: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "a".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "p".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                        op: Minus,
                                                        right: Box::new(BinaryOp {
                                                            left: Box::new(CompoundIdentifier(vec![
                                                                Ident {
                                                                    value: "a".to_string(),
                                                                    quote_style: None,
                                                                },
                                                                Ident {
                                                                    value: "p".to_string(),
                                                                    quote_style: None,
                                                                },
                                                            ])),
                                                            op: Divide,
                                                            right: Box::new(Value(number("1"))),
                                                        }),
                                                    }),
                                                    Unnamed(BinaryOp {
                                                        left: Box::new(Nested(vec![BinaryOp {
                                                            left: Box::new(CompoundIdentifier(vec![
                                                                Ident {
                                                                    value: "a".to_string(),
                                                                    quote_style: None,
                                                                },
                                                                Ident {
                                                                    value: "p".to_string(),
                                                                    quote_style: None,
                                                                },
                                                            ])),
                                                            op: Minus,
                                                            right: Box::new(CompoundIdentifier(vec![
                                                                Ident {
                                                                    value: "b".to_string(),
                                                                    quote_style: None,
                                                                },
                                                                Ident {
                                                                    value: "p".to_string(),
                                                                    quote_style: None,
                                                                },
                                                            ])),
                                                        }])),
                                                        op: Divide,
                                                        right: Box::new(CompoundIdentifier(vec![
                                                            Ident {
                                                                value: "a".to_string(),
                                                                quote_style: None,
                                                            },
                                                            Ident {
                                                                value: "p".to_string(),
                                                                quote_style: None,
                                                            },
                                                        ])),
                                                    }),
                                                ],
                                                within_group: vec![],
                                                over: None,
                                                distinct: false,
                                                ignore_respect_nulls: None,
                                                order_by: vec![],
                                                limit: None,
                                                outer_ignore_respect_nulls: None,
                                            })),
                                        ],
                                        within_group: vec![],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "rate_of_change".to_string(),
                                        quote_style: None,
                                    },
                                },
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![Ident {
                                        value: "years_included".to_string(),
                                        quote_style: None,
                                    }]),
                                    alias: Some(TableAlias {
                                        name: Ident {
                                            value: "a".to_string(),
                                            quote_style: None,
                                        },
                                        columns: vec![],
                                    }),
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![Join {
                                    relation: Table {
                                        name: ObjectName(vec![Ident {
                                            value: "years_included".to_string(),
                                            quote_style: None,
                                        }]),
                                        alias: Some(TableAlias {
                                            name: Ident {
                                                value: "b".to_string(),
                                                quote_style: None,
                                            },
                                            columns: vec![],
                                        }),
                                        args: vec![],
                                        with_hints: vec![],
                                    },
                                    join_operator: LeftOuter(On(BinaryOp {
                                        left: Box::new(BinaryOp {
                                            left: Box::new(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "b".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "owner_id".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                            op: Eq,
                                            right: Box::new(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "a".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "owner_id".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                        }),
                                        op: And,
                                        right: Box::new(BinaryOp {
                                            left: Box::new(CompoundIdentifier(vec![
                                                Ident {
                                                    value: "b".to_string(),
                                                    quote_style: None,
                                                },
                                                Ident {
                                                    value: "active_month".to_string(),
                                                    quote_style: None,
                                                },
                                            ])),
                                            op: Eq,
                                            right: Box::new(BinaryOp {
                                                left: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "a".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "active_month".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                                op: Minus,
                                                right: Box::new(Value(number("1"))),
                                            }),
                                        }),
                                    })),
                                }],
                            }],
                            selection: Some(BinaryOp {
                                left: Box::new(CompoundIdentifier(vec![
                                    Ident {
                                        value: "a".to_string(),
                                        quote_style: None,
                                    },
                                    Ident {
                                        value: "active_month".to_string(),
                                        quote_style: None,
                                    },
                                ])),
                                op: LtEq,
                                right: Box::new(Value(number("24"))),
                            }),
                            group_by: vec![],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![
                            OrderByExpr {
                                expr: Identifier(Ident {
                                    value: "name".to_string(),
                                    quote_style: None,
                                }),
                                asc: None,
                                nulls_first: None,
                            },
                            OrderByExpr {
                                expr: Identifier(Ident {
                                    value: "active_month".to_string(),
                                    quote_style: None,
                                }),
                                asc: None,
                                nulls_first: None,
                            },
                        ],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "percents".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![
                                UnnamedExpr(Identifier(Ident {
                                    value: "segment_c".to_string(),
                                    quote_style: None,
                                })),
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "PERCENTILE_CONT".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![Unnamed(Value(number(".80")))],
                                        within_group: vec![OrderByExpr {
                                            expr: Identifier(Ident {
                                                value: "max_growth".to_string(),
                                                quote_style: None,
                                            }),
                                            asc: None,
                                            nulls_first: None,
                                        }],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "p80".to_string(),
                                        quote_style: None,
                                    },
                                },
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![Ident {
                                        value: "all_reps".to_string(),
                                        quote_style: None,
                                    }]),
                                    alias: None,
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![],
                            }],
                            selection: None,
                            group_by: vec![Identifier(Ident {
                                value: "segment_c".to_string(),
                                quote_style: None,
                            })],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "percents2".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![
                                UnnamedExpr(Identifier(Ident {
                                    value: "segment_c".to_string(),
                                    quote_style: None,
                                })),
                                UnnamedExpr(Identifier(Ident {
                                    value: "region".to_string(),
                                    quote_style: None,
                                })),
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "PERCENTILE_CONT".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![Unnamed(Value(number(".8")))],
                                        within_group: vec![OrderByExpr {
                                            expr: Identifier(Ident {
                                                value: "max_growth".to_string(),
                                                quote_style: None,
                                            }),
                                            asc: None,
                                            nulls_first: None,
                                        }],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "p80".to_string(),
                                        quote_style: None,
                                    },
                                },
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![Ident {
                                        value: "all_reps".to_string(),
                                        quote_style: None,
                                    }]),
                                    alias: None,
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![],
                            }],
                            selection: None,
                            group_by: vec![
                                Identifier(Ident {
                                    value: "segment_c".to_string(),
                                    quote_style: None,
                                }),
                                Identifier(Ident {
                                    value: "region".to_string(),
                                    quote_style: None,
                                }),
                            ],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
                Cte {
                    alias: TableAlias {
                        name: Ident {
                            value: "temp".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    },
                    query: Query {
                        with: None,
                        body: Select(Box::new(Select {
                            distinct: true,
                            top: None,
                            projection: vec![
                                SelectItem::Wildcard {
                                    prefix: Some(ObjectName(vec![Ident {
                                        value: "a".to_string(),
                                        quote_style: None,
                                    }])),
                                    except: vec![],
                                    replace: vec![],
                                },
                                ExprWithAlias {
                                    expr: Expr::Function(Function {
                                        name: ObjectName(vec![Ident {
                                            value: "iff".to_string(),
                                            quote_style: None,
                                        }]),
                                        args: vec![
                                            Unnamed(BinaryOp {
                                                left: Box::new(Identifier(Ident {
                                                    value: "max_growth".to_string(),
                                                    quote_style: None,
                                                })),
                                                op: GtEq,
                                                right: Box::new(CompoundIdentifier(vec![
                                                    Ident {
                                                        value: "p".to_string(),
                                                        quote_style: None,
                                                    },
                                                    Ident {
                                                        value: "p80".to_string(),
                                                        quote_style: None,
                                                    },
                                                ])),
                                            }),
                                            Unnamed(Value(number("1"))),
                                            Unnamed(Value(number("0"))),
                                        ],
                                        within_group: vec![],
                                        over: None,
                                        distinct: false,
                                        ignore_respect_nulls: None,
                                        order_by: vec![],
                                        limit: None,
                                        outer_ignore_respect_nulls: None,
                                    }),
                                    alias: Ident {
                                        value: "outlier_by_segment".to_string(),
                                        quote_style: None,
                                    },
                                },
                            ],
                            from: vec![TableWithJoins {
                                relation: Table {
                                    name: ObjectName(vec![Ident {
                                        value: "all_reps".to_string(),
                                        quote_style: None,
                                    }]),
                                    alias: Some(TableAlias {
                                        name: Ident {
                                            value: "a".to_string(),
                                            quote_style: None,
                                        },
                                        columns: vec![],
                                    }),
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![Join {
                                    relation: Table {
                                        name: ObjectName(vec![Ident {
                                            value: "percents".to_string(),
                                            quote_style: None,
                                        }]),
                                        alias: Some(TableAlias {
                                            name: Ident {
                                                value: "p".to_string(),
                                                quote_style: None,
                                            },
                                            columns: vec![],
                                        }),
                                        args: vec![],
                                        with_hints: vec![],
                                    },
                                    join_operator: LeftOuter(On(BinaryOp {
                                        left: Box::new(CompoundIdentifier(vec![
                                            Ident {
                                                value: "p".to_string(),
                                                quote_style: None,
                                            },
                                            Ident {
                                                value: "segment_c".to_string(),
                                                quote_style: None,
                                            },
                                        ])),
                                        op: Eq,
                                        right: Box::new(CompoundIdentifier(vec![
                                            Ident {
                                                value: "a".to_string(),
                                                quote_style: None,
                                            },
                                            Ident {
                                                value: "segment_c".to_string(),
                                                quote_style: None,
                                            },
                                        ])),
                                    })),
                                }],
                            }],
                            selection: None,
                            group_by: vec![],
                            having: None,
                            qualify: None,
                            windows: vec![],
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                },
            ],
        }),
        body: Select(Box::new(Select {
            distinct: false,
            top: None,
            projection: vec![
                SelectItem::Wildcard {
                    prefix: Some(ObjectName(vec![Ident {
                        value: "a".to_string(),
                        quote_style: None,
                    }])),
                    except: vec![],
                    replace: vec![],
                },
                ExprWithAlias {
                    expr: Expr::Function(Function {
                        name: ObjectName(vec![Ident {
                            value: "iff".to_string(),
                            quote_style: None,
                        }]),
                        args: vec![
                            Unnamed(BinaryOp {
                                left: Box::new(Identifier(Ident {
                                    value: "max_growth".to_string(),
                                    quote_style: None,
                                })),
                                op: GtEq,
                                right: Box::new(CompoundIdentifier(vec![
                                    Ident {
                                        value: "l".to_string(),
                                        quote_style: None,
                                    },
                                    Ident {
                                        value: "p80".to_string(),
                                        quote_style: None,
                                    },
                                ])),
                            }),
                            Unnamed(Value(number("1"))),
                            Unnamed(Value(number("0"))),
                        ],
                        within_group: vec![],
                        over: None,
                        distinct: false,
                        ignore_respect_nulls: None,
                        order_by: vec![],
                        limit: None,
                        outer_ignore_respect_nulls: None,
                    }),
                    alias: Ident {
                        value: "outlier_by_region".to_string(),
                        quote_style: None,
                    },
                },
            ],
            from: vec![TableWithJoins {
                relation: Table {
                    name: ObjectName(vec![Ident {
                        value: "temp".to_string(),
                        quote_style: None,
                    }]),
                    alias: Some(TableAlias {
                        name: Ident {
                            value: "a".to_string(),
                            quote_style: None,
                        },
                        columns: vec![],
                    }),
                    args: vec![],
                    with_hints: vec![],
                },
                joins: vec![Join {
                    relation: Table {
                        name: ObjectName(vec![Ident {
                            value: "percents2".to_string(),
                            quote_style: None,
                        }]),
                        alias: Some(TableAlias {
                            name: Ident {
                                value: "l".to_string(),
                                quote_style: None,
                            },
                            columns: vec![],
                        }),
                        args: vec![],
                        with_hints: vec![],
                    },
                    join_operator: LeftOuter(On(BinaryOp {
                        left: Box::new(BinaryOp {
                            left: Box::new(CompoundIdentifier(vec![
                                Ident {
                                    value: "a".to_string(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "region".to_string(),
                                    quote_style: None,
                                },
                            ])),
                            op: Eq,
                            right: Box::new(CompoundIdentifier(vec![
                                Ident {
                                    value: "l".to_string(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "region".to_string(),
                                    quote_style: None,
                                },
                            ])),
                        }),
                        op: And,
                        right: Box::new(BinaryOp {
                            left: Box::new(CompoundIdentifier(vec![
                                Ident {
                                    value: "a".to_string(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "segment_c".to_string(),
                                    quote_style: None,
                                },
                            ])),
                            op: Eq,
                            right: Box::new(CompoundIdentifier(vec![
                                Ident {
                                    value: "l".to_string(),
                                    quote_style: None,
                                },
                                Ident {
                                    value: "segment_c".to_string(),
                                    quote_style: None,
                                },
                            ])),
                        }),
                    })),
                }],
            }],
            selection: None,
            group_by: vec![],
            having: None,
            qualify: None,
            windows: vec![],
        })),
        order_by: vec![
            OrderByExpr {
                expr: Identifier(Ident {
                    value: "name".to_string(),
                    quote_style: None,
                }),
                asc: Some(true),
                nulls_first: None,
            },
            OrderByExpr {
                expr: Identifier(Ident {
                    value: "active_month".to_string(),
                    quote_style: None,
                }),
                asc: Some(true),
                nulls_first: None,
            },
        ],
        limit: None,
        offset: None,
        fetch: None,
    }))];
    assert_eq!(actual_res, expected);
}
