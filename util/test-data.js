const srcText = `#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Query {
    /// WITH (common table expressions, or CTEs)
    pub ctes: Vec<Cte>,
    /// SELECT or UNION / EXCEPT / INTECEPT
    pub body: SetExpr,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr>,
    /// \`LIMIT { <N> | ALL }\`
    pub limit: Option<Expr>,
    /// \`OFFSET <N> [ {ROW | ROWS} ]\`
    pub offset: Option<Offset>,
    /// \`FETCH {FIRST | NEXT} <N> [ PERCENT ] {ROW | ROWS} | {ONLY | WITH TIES }\`
    pub fetch: Option<Fetch>,
}`

const json = {
  "_type": "File",
  "attrs": [],
  "items": [
    {
      "_type": "ItemStruct",
      "attrs": [
        {
          "_type": "Attribute",
          "pound_token": {
            "_type": "Pound",
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 1,
                "column": 0
              },
              "end": {
                "_type": "LineColumn",
                "line": 1,
                "column": 1
              }
            }
          },
          "style": {
            "_type": "AttrStyle::Outer"
          },
          "path": {
            "_type": "Path",
            "segments": {
              "0": {
                "_type": "PathSegment",
                "ident": {
                  "_type": "Ident",
                  "to_string": "derive",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 2
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 8
                    }
                  }
                },
                "arguments": {
                  "_type": "PathArguments::None"
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 1,
                    "column": 2
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 1,
                    "column": 8
                  }
                }
              },
              "_type": "Punctuated",
              "length": 1
            },
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 1,
                "column": 2
              },
              "end": {
                "_type": "LineColumn",
                "line": 1,
                "column": 8
              }
            }
          },
          "tts": {
            "0": {
              "_type": "Group",
              "delimiter": {
                "_type": "Delimiter::Parenthesis"
              },
              "stream": {
                "0": {
                  "_type": "Ident",
                  "to_string": "Debug",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 9
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 14
                    }
                  }
                },
                "1": {
                  "_type": "Punct",
                  "as_char": ",",
                  "spacing": {
                    "_type": "Spacing::Alone"
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 14
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 15
                    }
                  }
                },
                "2": {
                  "_type": "Ident",
                  "to_string": "Clone",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 16
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 21
                    }
                  }
                },
                "3": {
                  "_type": "Punct",
                  "as_char": ",",
                  "spacing": {
                    "_type": "Spacing::Alone"
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 21
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 22
                    }
                  }
                },
                "4": {
                  "_type": "Ident",
                  "to_string": "PartialEq",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 23
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 32
                    }
                  }
                },
                "5": {
                  "_type": "Punct",
                  "as_char": ",",
                  "spacing": {
                    "_type": "Spacing::Alone"
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 32
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 33
                    }
                  }
                },
                "6": {
                  "_type": "Ident",
                  "to_string": "Eq",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 34
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 36
                    }
                  }
                },
                "7": {
                  "_type": "Punct",
                  "as_char": ",",
                  "spacing": {
                    "_type": "Spacing::Alone"
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 36
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 37
                    }
                  }
                },
                "8": {
                  "_type": "Ident",
                  "to_string": "Hash",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 38
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 42
                    }
                  }
                },
                "9": {
                  "_type": "Punct",
                  "as_char": ",",
                  "spacing": {
                    "_type": "Spacing::Alone"
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 42
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 43
                    }
                  }
                },
                "10": {
                  "_type": "Ident",
                  "to_string": "Serialize",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 44
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 53
                    }
                  }
                },
                "11": {
                  "_type": "Punct",
                  "as_char": ",",
                  "spacing": {
                    "_type": "Spacing::Alone"
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 53
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 54
                    }
                  }
                },
                "12": {
                  "_type": "Ident",
                  "to_string": "Deserialize",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 55
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 1,
                      "column": 66
                    }
                  }
                },
                "_type": "TokenStream",
                "length": 13
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 1,
                  "column": 8
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 1,
                  "column": 67
                }
              }
            },
            "_type": "TokenStream",
            "length": 1
          },
          "bracket_token": {
            "_type": "Bracket",
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 1,
                "column": 1
              },
              "end": {
                "_type": "LineColumn",
                "line": 1,
                "column": 68
              }
            }
          },
          "span": {
            "_type": "Span",
            "start": {
              "_type": "LineColumn",
              "line": 1,
              "column": 0
            },
            "end": {
              "_type": "LineColumn",
              "line": 1,
              "column": 68
            }
          }
        }
      ],
      "vis": {
        "_type": "VisPublic",
        "pub_token": {
          "_type": "Pub",
          "span": {
            "_type": "Span",
            "start": {
              "_type": "LineColumn",
              "line": 2,
              "column": 0
            },
            "end": {
              "_type": "LineColumn",
              "line": 2,
              "column": 3
            }
          }
        },
        "span": {
          "_type": "Span",
          "start": {
            "_type": "LineColumn",
            "line": 2,
            "column": 0
          },
          "end": {
            "_type": "LineColumn",
            "line": 2,
            "column": 3
          }
        }
      },
      "struct_token": {
        "_type": "Struct",
        "span": {
          "_type": "Span",
          "start": {
            "_type": "LineColumn",
            "line": 2,
            "column": 4
          },
          "end": {
            "_type": "LineColumn",
            "line": 2,
            "column": 10
          }
        }
      },
      "ident": {
        "_type": "Ident",
        "to_string": "Query",
        "span": {
          "_type": "Span",
          "start": {
            "_type": "LineColumn",
            "line": 2,
            "column": 11
          },
          "end": {
            "_type": "LineColumn",
            "line": 2,
            "column": 16
          }
        }
      },
      "generics": {
        "_type": "Generics",
        "params": {
          "_type": "Punctuated",
          "length": 0
        },
        "span": {
          "_type": "Span",
          "start": {
            "_type": "LineColumn",
            "line": 1,
            "column": 0
          },
          "end": {
            "_type": "LineColumn",
            "line": 1,
            "column": 0
          }
        }
      },
      "fields": {
        "_type": "FieldsNamed",
        "named": {
          "0": {
            "_type": "Field",
            "attrs": [
              {
                "_type": "Attribute",
                "pound_token": {
                  "_type": "Pound",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 3,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 3,
                      "column": 48
                    }
                  }
                },
                "style": {
                  "_type": "AttrStyle::Outer"
                },
                "path": {
                  "_type": "Path",
                  "segments": {
                    "0": {
                      "_type": "PathSegment",
                      "ident": {
                        "_type": "Ident",
                        "to_string": "doc",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 3,
                            "column": 4
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 3,
                            "column": 48
                          }
                        }
                      },
                      "arguments": {
                        "_type": "PathArguments::None"
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 3,
                          "column": 4
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 3,
                          "column": 48
                        }
                      }
                    },
                    "_type": "Punctuated",
                    "length": 1
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 3,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 3,
                      "column": 48
                    }
                  }
                },
                "tts": {
                  "0": {
                    "_type": "Punct",
                    "as_char": "=",
                    "spacing": {
                      "_type": "Spacing::Alone"
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 3,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 3,
                        "column": 48
                      }
                    }
                  },
                  "1": {
                    "_type": "Literal",
                    "to_string": "\" WITH (common table expressions, or CTEs)\"",
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 3,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 3,
                        "column": 48
                      }
                    }
                  },
                  "_type": "TokenStream",
                  "length": 2
                },
                "bracket_token": {
                  "_type": "Bracket",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 3,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 3,
                      "column": 48
                    }
                  }
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 3,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 3,
                    "column": 48
                  }
                }
              }
            ],
            "vis": {
              "_type": "VisPublic",
              "pub_token": {
                "_type": "Pub",
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 4,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 4,
                    "column": 7
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 4,
                  "column": 4
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 4,
                  "column": 7
                }
              }
            },
            "ident": {
              "_type": "Ident",
              "to_string": "ctes",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 4,
                  "column": 8
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 4,
                  "column": 12
                }
              }
            },
            "colon_token": {
              "_type": "Colon",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 4,
                  "column": 12
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 4,
                  "column": 13
                }
              }
            },
            "ty": {
              "_type": "TypePath",
              "path": {
                "_type": "Path",
                "segments": {
                  "0": {
                    "_type": "PathSegment",
                    "ident": {
                      "_type": "Ident",
                      "to_string": "Vec",
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 4,
                          "column": 14
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 4,
                          "column": 17
                        }
                      }
                    },
                    "arguments": {
                      "_type": "AngleBracketedGenericArguments",
                      "lt_token": {
                        "_type": "Lt",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 4,
                            "column": 17
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 4,
                            "column": 18
                          }
                        }
                      },
                      "args": {
                        "0": {
                          "_type": "TypePath",
                          "path": {
                            "_type": "Path",
                            "segments": {
                              "0": {
                                "_type": "PathSegment",
                                "ident": {
                                  "_type": "Ident",
                                  "to_string": "Cte",
                                  "span": {
                                    "_type": "Span",
                                    "start": {
                                      "_type": "LineColumn",
                                      "line": 4,
                                      "column": 18
                                    },
                                    "end": {
                                      "_type": "LineColumn",
                                      "line": 4,
                                      "column": 21
                                    }
                                  }
                                },
                                "arguments": {
                                  "_type": "PathArguments::None"
                                },
                                "span": {
                                  "_type": "Span",
                                  "start": {
                                    "_type": "LineColumn",
                                    "line": 4,
                                    "column": 18
                                  },
                                  "end": {
                                    "_type": "LineColumn",
                                    "line": 4,
                                    "column": 21
                                  }
                                }
                              },
                              "_type": "Punctuated",
                              "length": 1
                            },
                            "span": {
                              "_type": "Span",
                              "start": {
                                "_type": "LineColumn",
                                "line": 4,
                                "column": 18
                              },
                              "end": {
                                "_type": "LineColumn",
                                "line": 4,
                                "column": 21
                              }
                            }
                          },
                          "span": {
                            "_type": "Span",
                            "start": {
                              "_type": "LineColumn",
                              "line": 4,
                              "column": 18
                            },
                            "end": {
                              "_type": "LineColumn",
                              "line": 4,
                              "column": 21
                            }
                          }
                        },
                        "_type": "Punctuated",
                        "length": 1
                      },
                      "gt_token": {
                        "_type": "Gt",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 4,
                            "column": 21
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 4,
                            "column": 22
                          }
                        }
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 4,
                          "column": 17
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 4,
                          "column": 22
                        }
                      }
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 4,
                        "column": 14
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 4,
                        "column": 22
                      }
                    }
                  },
                  "_type": "Punctuated",
                  "length": 1
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 4,
                    "column": 14
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 4,
                    "column": 22
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 4,
                  "column": 14
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 4,
                  "column": 22
                }
              }
            },
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 3,
                "column": 4
              },
              "end": {
                "_type": "LineColumn",
                "line": 4,
                "column": 22
              }
            }
          },
          "1": {
            "_type": "Comma",
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 4,
                "column": 22
              },
              "end": {
                "_type": "LineColumn",
                "line": 4,
                "column": 23
              }
            }
          },
          "2": {
            "_type": "Field",
            "attrs": [
              {
                "_type": "Attribute",
                "pound_token": {
                  "_type": "Pound",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 5,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 5,
                      "column": 43
                    }
                  }
                },
                "style": {
                  "_type": "AttrStyle::Outer"
                },
                "path": {
                  "_type": "Path",
                  "segments": {
                    "0": {
                      "_type": "PathSegment",
                      "ident": {
                        "_type": "Ident",
                        "to_string": "doc",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 5,
                            "column": 4
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 5,
                            "column": 43
                          }
                        }
                      },
                      "arguments": {
                        "_type": "PathArguments::None"
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 5,
                          "column": 4
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 5,
                          "column": 43
                        }
                      }
                    },
                    "_type": "Punctuated",
                    "length": 1
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 5,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 5,
                      "column": 43
                    }
                  }
                },
                "tts": {
                  "0": {
                    "_type": "Punct",
                    "as_char": "=",
                    "spacing": {
                      "_type": "Spacing::Alone"
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 5,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 5,
                        "column": 43
                      }
                    }
                  },
                  "1": {
                    "_type": "Literal",
                    "to_string": "\" SELECT or UNION / EXCEPT / INTECEPT\"",
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 5,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 5,
                        "column": 43
                      }
                    }
                  },
                  "_type": "TokenStream",
                  "length": 2
                },
                "bracket_token": {
                  "_type": "Bracket",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 5,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 5,
                      "column": 43
                    }
                  }
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 5,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 5,
                    "column": 43
                  }
                }
              }
            ],
            "vis": {
              "_type": "VisPublic",
              "pub_token": {
                "_type": "Pub",
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 6,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 6,
                    "column": 7
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 6,
                  "column": 4
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 6,
                  "column": 7
                }
              }
            },
            "ident": {
              "_type": "Ident",
              "to_string": "body",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 6,
                  "column": 8
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 6,
                  "column": 12
                }
              }
            },
            "colon_token": {
              "_type": "Colon",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 6,
                  "column": 12
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 6,
                  "column": 13
                }
              }
            },
            "ty": {
              "_type": "TypePath",
              "path": {
                "_type": "Path",
                "segments": {
                  "0": {
                    "_type": "PathSegment",
                    "ident": {
                      "_type": "Ident",
                      "to_string": "SetExpr",
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 6,
                          "column": 14
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 6,
                          "column": 21
                        }
                      }
                    },
                    "arguments": {
                      "_type": "PathArguments::None"
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 6,
                        "column": 14
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 6,
                        "column": 21
                      }
                    }
                  },
                  "_type": "Punctuated",
                  "length": 1
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 6,
                    "column": 14
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 6,
                    "column": 21
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 6,
                  "column": 14
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 6,
                  "column": 21
                }
              }
            },
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 5,
                "column": 4
              },
              "end": {
                "_type": "LineColumn",
                "line": 6,
                "column": 21
              }
            }
          },
          "3": {
            "_type": "Comma",
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 6,
                "column": 21
              },
              "end": {
                "_type": "LineColumn",
                "line": 6,
                "column": 22
              }
            }
          },
          "4": {
            "_type": "Field",
            "attrs": [
              {
                "_type": "Attribute",
                "pound_token": {
                  "_type": "Pound",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 7,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 7,
                      "column": 16
                    }
                  }
                },
                "style": {
                  "_type": "AttrStyle::Outer"
                },
                "path": {
                  "_type": "Path",
                  "segments": {
                    "0": {
                      "_type": "PathSegment",
                      "ident": {
                        "_type": "Ident",
                        "to_string": "doc",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 7,
                            "column": 4
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 7,
                            "column": 16
                          }
                        }
                      },
                      "arguments": {
                        "_type": "PathArguments::None"
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 7,
                          "column": 4
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 7,
                          "column": 16
                        }
                      }
                    },
                    "_type": "Punctuated",
                    "length": 1
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 7,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 7,
                      "column": 16
                    }
                  }
                },
                "tts": {
                  "0": {
                    "_type": "Punct",
                    "as_char": "=",
                    "spacing": {
                      "_type": "Spacing::Alone"
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 7,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 7,
                        "column": 16
                      }
                    }
                  },
                  "1": {
                    "_type": "Literal",
                    "to_string": "\" ORDER BY\"",
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 7,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 7,
                        "column": 16
                      }
                    }
                  },
                  "_type": "TokenStream",
                  "length": 2
                },
                "bracket_token": {
                  "_type": "Bracket",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 7,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 7,
                      "column": 16
                    }
                  }
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 7,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 7,
                    "column": 16
                  }
                }
              }
            ],
            "vis": {
              "_type": "VisPublic",
              "pub_token": {
                "_type": "Pub",
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 8,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 8,
                    "column": 7
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 8,
                  "column": 4
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 8,
                  "column": 7
                }
              }
            },
            "ident": {
              "_type": "Ident",
              "to_string": "order_by",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 8,
                  "column": 8
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 8,
                  "column": 16
                }
              }
            },
            "colon_token": {
              "_type": "Colon",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 8,
                  "column": 16
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 8,
                  "column": 17
                }
              }
            },
            "ty": {
              "_type": "TypePath",
              "path": {
                "_type": "Path",
                "segments": {
                  "0": {
                    "_type": "PathSegment",
                    "ident": {
                      "_type": "Ident",
                      "to_string": "Vec",
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 8,
                          "column": 18
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 8,
                          "column": 21
                        }
                      }
                    },
                    "arguments": {
                      "_type": "AngleBracketedGenericArguments",
                      "lt_token": {
                        "_type": "Lt",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 8,
                            "column": 21
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 8,
                            "column": 22
                          }
                        }
                      },
                      "args": {
                        "0": {
                          "_type": "TypePath",
                          "path": {
                            "_type": "Path",
                            "segments": {
                              "0": {
                                "_type": "PathSegment",
                                "ident": {
                                  "_type": "Ident",
                                  "to_string": "OrderByExpr",
                                  "span": {
                                    "_type": "Span",
                                    "start": {
                                      "_type": "LineColumn",
                                      "line": 8,
                                      "column": 22
                                    },
                                    "end": {
                                      "_type": "LineColumn",
                                      "line": 8,
                                      "column": 33
                                    }
                                  }
                                },
                                "arguments": {
                                  "_type": "PathArguments::None"
                                },
                                "span": {
                                  "_type": "Span",
                                  "start": {
                                    "_type": "LineColumn",
                                    "line": 8,
                                    "column": 22
                                  },
                                  "end": {
                                    "_type": "LineColumn",
                                    "line": 8,
                                    "column": 33
                                  }
                                }
                              },
                              "_type": "Punctuated",
                              "length": 1
                            },
                            "span": {
                              "_type": "Span",
                              "start": {
                                "_type": "LineColumn",
                                "line": 8,
                                "column": 22
                              },
                              "end": {
                                "_type": "LineColumn",
                                "line": 8,
                                "column": 33
                              }
                            }
                          },
                          "span": {
                            "_type": "Span",
                            "start": {
                              "_type": "LineColumn",
                              "line": 8,
                              "column": 22
                            },
                            "end": {
                              "_type": "LineColumn",
                              "line": 8,
                              "column": 33
                            }
                          }
                        },
                        "_type": "Punctuated",
                        "length": 1
                      },
                      "gt_token": {
                        "_type": "Gt",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 8,
                            "column": 33
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 8,
                            "column": 34
                          }
                        }
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 8,
                          "column": 21
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 8,
                          "column": 34
                        }
                      }
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 8,
                        "column": 18
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 8,
                        "column": 34
                      }
                    }
                  },
                  "_type": "Punctuated",
                  "length": 1
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 8,
                    "column": 18
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 8,
                    "column": 34
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 8,
                  "column": 18
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 8,
                  "column": 34
                }
              }
            },
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 7,
                "column": 4
              },
              "end": {
                "_type": "LineColumn",
                "line": 8,
                "column": 34
              }
            }
          },
          "5": {
            "_type": "Comma",
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 8,
                "column": 34
              },
              "end": {
                "_type": "LineColumn",
                "line": 8,
                "column": 35
              }
            }
          },
          "6": {
            "_type": "Field",
            "attrs": [
              {
                "_type": "Attribute",
                "pound_token": {
                  "_type": "Pound",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 9,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 9,
                      "column": 29
                    }
                  }
                },
                "style": {
                  "_type": "AttrStyle::Outer"
                },
                "path": {
                  "_type": "Path",
                  "segments": {
                    "0": {
                      "_type": "PathSegment",
                      "ident": {
                        "_type": "Ident",
                        "to_string": "doc",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 9,
                            "column": 4
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 9,
                            "column": 29
                          }
                        }
                      },
                      "arguments": {
                        "_type": "PathArguments::None"
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 9,
                          "column": 4
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 9,
                          "column": 29
                        }
                      }
                    },
                    "_type": "Punctuated",
                    "length": 1
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 9,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 9,
                      "column": 29
                    }
                  }
                },
                "tts": {
                  "0": {
                    "_type": "Punct",
                    "as_char": "=",
                    "spacing": {
                      "_type": "Spacing::Alone"
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 9,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 9,
                        "column": 29
                      }
                    }
                  },
                  "1": {
                    "_type": "Literal",
                    "to_string": "\" `LIMIT { <N> | ALL }`\"",
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 9,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 9,
                        "column": 29
                      }
                    }
                  },
                  "_type": "TokenStream",
                  "length": 2
                },
                "bracket_token": {
                  "_type": "Bracket",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 9,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 9,
                      "column": 29
                    }
                  }
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 9,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 9,
                    "column": 29
                  }
                }
              }
            ],
            "vis": {
              "_type": "VisPublic",
              "pub_token": {
                "_type": "Pub",
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 10,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 10,
                    "column": 7
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 10,
                  "column": 4
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 10,
                  "column": 7
                }
              }
            },
            "ident": {
              "_type": "Ident",
              "to_string": "limit",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 10,
                  "column": 8
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 10,
                  "column": 13
                }
              }
            },
            "colon_token": {
              "_type": "Colon",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 10,
                  "column": 13
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 10,
                  "column": 14
                }
              }
            },
            "ty": {
              "_type": "TypePath",
              "path": {
                "_type": "Path",
                "segments": {
                  "0": {
                    "_type": "PathSegment",
                    "ident": {
                      "_type": "Ident",
                      "to_string": "Option",
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 10,
                          "column": 15
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 10,
                          "column": 21
                        }
                      }
                    },
                    "arguments": {
                      "_type": "AngleBracketedGenericArguments",
                      "lt_token": {
                        "_type": "Lt",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 10,
                            "column": 21
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 10,
                            "column": 22
                          }
                        }
                      },
                      "args": {
                        "0": {
                          "_type": "TypePath",
                          "path": {
                            "_type": "Path",
                            "segments": {
                              "0": {
                                "_type": "PathSegment",
                                "ident": {
                                  "_type": "Ident",
                                  "to_string": "Expr",
                                  "span": {
                                    "_type": "Span",
                                    "start": {
                                      "_type": "LineColumn",
                                      "line": 10,
                                      "column": 22
                                    },
                                    "end": {
                                      "_type": "LineColumn",
                                      "line": 10,
                                      "column": 26
                                    }
                                  }
                                },
                                "arguments": {
                                  "_type": "PathArguments::None"
                                },
                                "span": {
                                  "_type": "Span",
                                  "start": {
                                    "_type": "LineColumn",
                                    "line": 10,
                                    "column": 22
                                  },
                                  "end": {
                                    "_type": "LineColumn",
                                    "line": 10,
                                    "column": 26
                                  }
                                }
                              },
                              "_type": "Punctuated",
                              "length": 1
                            },
                            "span": {
                              "_type": "Span",
                              "start": {
                                "_type": "LineColumn",
                                "line": 10,
                                "column": 22
                              },
                              "end": {
                                "_type": "LineColumn",
                                "line": 10,
                                "column": 26
                              }
                            }
                          },
                          "span": {
                            "_type": "Span",
                            "start": {
                              "_type": "LineColumn",
                              "line": 10,
                              "column": 22
                            },
                            "end": {
                              "_type": "LineColumn",
                              "line": 10,
                              "column": 26
                            }
                          }
                        },
                        "_type": "Punctuated",
                        "length": 1
                      },
                      "gt_token": {
                        "_type": "Gt",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 10,
                            "column": 26
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 10,
                            "column": 27
                          }
                        }
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 10,
                          "column": 21
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 10,
                          "column": 27
                        }
                      }
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 10,
                        "column": 15
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 10,
                        "column": 27
                      }
                    }
                  },
                  "_type": "Punctuated",
                  "length": 1
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 10,
                    "column": 15
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 10,
                    "column": 27
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 10,
                  "column": 15
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 10,
                  "column": 27
                }
              }
            },
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 9,
                "column": 4
              },
              "end": {
                "_type": "LineColumn",
                "line": 10,
                "column": 27
              }
            }
          },
          "7": {
            "_type": "Comma",
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 10,
                "column": 27
              },
              "end": {
                "_type": "LineColumn",
                "line": 10,
                "column": 28
              }
            }
          },
          "8": {
            "_type": "Field",
            "attrs": [
              {
                "_type": "Attribute",
                "pound_token": {
                  "_type": "Pound",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 11,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 11,
                      "column": 39
                    }
                  }
                },
                "style": {
                  "_type": "AttrStyle::Outer"
                },
                "path": {
                  "_type": "Path",
                  "segments": {
                    "0": {
                      "_type": "PathSegment",
                      "ident": {
                        "_type": "Ident",
                        "to_string": "doc",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 11,
                            "column": 4
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 11,
                            "column": 39
                          }
                        }
                      },
                      "arguments": {
                        "_type": "PathArguments::None"
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 11,
                          "column": 4
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 11,
                          "column": 39
                        }
                      }
                    },
                    "_type": "Punctuated",
                    "length": 1
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 11,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 11,
                      "column": 39
                    }
                  }
                },
                "tts": {
                  "0": {
                    "_type": "Punct",
                    "as_char": "=",
                    "spacing": {
                      "_type": "Spacing::Alone"
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 11,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 11,
                        "column": 39
                      }
                    }
                  },
                  "1": {
                    "_type": "Literal",
                    "to_string": "\" `OFFSET <N> [ { ROW | ROWS } ]`\"",
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 11,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 11,
                        "column": 39
                      }
                    }
                  },
                  "_type": "TokenStream",
                  "length": 2
                },
                "bracket_token": {
                  "_type": "Bracket",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 11,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 11,
                      "column": 39
                    }
                  }
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 11,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 11,
                    "column": 39
                  }
                }
              }
            ],
            "vis": {
              "_type": "VisPublic",
              "pub_token": {
                "_type": "Pub",
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 12,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 12,
                    "column": 7
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 12,
                  "column": 4
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 12,
                  "column": 7
                }
              }
            },
            "ident": {
              "_type": "Ident",
              "to_string": "offset",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 12,
                  "column": 8
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 12,
                  "column": 14
                }
              }
            },
            "colon_token": {
              "_type": "Colon",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 12,
                  "column": 14
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 12,
                  "column": 15
                }
              }
            },
            "ty": {
              "_type": "TypePath",
              "path": {
                "_type": "Path",
                "segments": {
                  "0": {
                    "_type": "PathSegment",
                    "ident": {
                      "_type": "Ident",
                      "to_string": "Option",
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 12,
                          "column": 16
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 12,
                          "column": 22
                        }
                      }
                    },
                    "arguments": {
                      "_type": "AngleBracketedGenericArguments",
                      "lt_token": {
                        "_type": "Lt",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 12,
                            "column": 22
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 12,
                            "column": 23
                          }
                        }
                      },
                      "args": {
                        "0": {
                          "_type": "TypePath",
                          "path": {
                            "_type": "Path",
                            "segments": {
                              "0": {
                                "_type": "PathSegment",
                                "ident": {
                                  "_type": "Ident",
                                  "to_string": "Offset",
                                  "span": {
                                    "_type": "Span",
                                    "start": {
                                      "_type": "LineColumn",
                                      "line": 12,
                                      "column": 23
                                    },
                                    "end": {
                                      "_type": "LineColumn",
                                      "line": 12,
                                      "column": 29
                                    }
                                  }
                                },
                                "arguments": {
                                  "_type": "PathArguments::None"
                                },
                                "span": {
                                  "_type": "Span",
                                  "start": {
                                    "_type": "LineColumn",
                                    "line": 12,
                                    "column": 23
                                  },
                                  "end": {
                                    "_type": "LineColumn",
                                    "line": 12,
                                    "column": 29
                                  }
                                }
                              },
                              "_type": "Punctuated",
                              "length": 1
                            },
                            "span": {
                              "_type": "Span",
                              "start": {
                                "_type": "LineColumn",
                                "line": 12,
                                "column": 23
                              },
                              "end": {
                                "_type": "LineColumn",
                                "line": 12,
                                "column": 29
                              }
                            }
                          },
                          "span": {
                            "_type": "Span",
                            "start": {
                              "_type": "LineColumn",
                              "line": 12,
                              "column": 23
                            },
                            "end": {
                              "_type": "LineColumn",
                              "line": 12,
                              "column": 29
                            }
                          }
                        },
                        "_type": "Punctuated",
                        "length": 1
                      },
                      "gt_token": {
                        "_type": "Gt",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 12,
                            "column": 29
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 12,
                            "column": 30
                          }
                        }
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 12,
                          "column": 22
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 12,
                          "column": 30
                        }
                      }
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 12,
                        "column": 16
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 12,
                        "column": 30
                      }
                    }
                  },
                  "_type": "Punctuated",
                  "length": 1
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 12,
                    "column": 16
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 12,
                    "column": 30
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 12,
                  "column": 16
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 12,
                  "column": 30
                }
              }
            },
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 11,
                "column": 4
              },
              "end": {
                "_type": "LineColumn",
                "line": 12,
                "column": 30
              }
            }
          },
          "9": {
            "_type": "Comma",
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 12,
                "column": 30
              },
              "end": {
                "_type": "LineColumn",
                "line": 12,
                "column": 31
              }
            }
          },
          "10": {
            "_type": "Field",
            "attrs": [
              {
                "_type": "Attribute",
                "pound_token": {
                  "_type": "Pound",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 13,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 13,
                      "column": 86
                    }
                  }
                },
                "style": {
                  "_type": "AttrStyle::Outer"
                },
                "path": {
                  "_type": "Path",
                  "segments": {
                    "0": {
                      "_type": "PathSegment",
                      "ident": {
                        "_type": "Ident",
                        "to_string": "doc",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 13,
                            "column": 4
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 13,
                            "column": 86
                          }
                        }
                      },
                      "arguments": {
                        "_type": "PathArguments::None"
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 13,
                          "column": 4
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 13,
                          "column": 86
                        }
                      }
                    },
                    "_type": "Punctuated",
                    "length": 1
                  },
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 13,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 13,
                      "column": 86
                    }
                  }
                },
                "tts": {
                  "0": {
                    "_type": "Punct",
                    "as_char": "=",
                    "spacing": {
                      "_type": "Spacing::Alone"
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 13,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 13,
                        "column": 86
                      }
                    }
                  },
                  "1": {
                    "_type": "Literal",
                    "to_string": "\" `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`\"",
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 13,
                        "column": 4
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 13,
                        "column": 86
                      }
                    }
                  },
                  "_type": "TokenStream",
                  "length": 2
                },
                "bracket_token": {
                  "_type": "Bracket",
                  "span": {
                    "_type": "Span",
                    "start": {
                      "_type": "LineColumn",
                      "line": 13,
                      "column": 4
                    },
                    "end": {
                      "_type": "LineColumn",
                      "line": 13,
                      "column": 86
                    }
                  }
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 13,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 13,
                    "column": 86
                  }
                }
              }
            ],
            "vis": {
              "_type": "VisPublic",
              "pub_token": {
                "_type": "Pub",
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 14,
                    "column": 4
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 14,
                    "column": 7
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 14,
                  "column": 4
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 14,
                  "column": 7
                }
              }
            },
            "ident": {
              "_type": "Ident",
              "to_string": "fetch",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 14,
                  "column": 8
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 14,
                  "column": 13
                }
              }
            },
            "colon_token": {
              "_type": "Colon",
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 14,
                  "column": 13
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 14,
                  "column": 14
                }
              }
            },
            "ty": {
              "_type": "TypePath",
              "path": {
                "_type": "Path",
                "segments": {
                  "0": {
                    "_type": "PathSegment",
                    "ident": {
                      "_type": "Ident",
                      "to_string": "Option",
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 14,
                          "column": 15
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 14,
                          "column": 21
                        }
                      }
                    },
                    "arguments": {
                      "_type": "AngleBracketedGenericArguments",
                      "lt_token": {
                        "_type": "Lt",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 14,
                            "column": 21
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 14,
                            "column": 22
                          }
                        }
                      },
                      "args": {
                        "0": {
                          "_type": "TypePath",
                          "path": {
                            "_type": "Path",
                            "segments": {
                              "0": {
                                "_type": "PathSegment",
                                "ident": {
                                  "_type": "Ident",
                                  "to_string": "Fetch",
                                  "span": {
                                    "_type": "Span",
                                    "start": {
                                      "_type": "LineColumn",
                                      "line": 14,
                                      "column": 22
                                    },
                                    "end": {
                                      "_type": "LineColumn",
                                      "line": 14,
                                      "column": 27
                                    }
                                  }
                                },
                                "arguments": {
                                  "_type": "PathArguments::None"
                                },
                                "span": {
                                  "_type": "Span",
                                  "start": {
                                    "_type": "LineColumn",
                                    "line": 14,
                                    "column": 22
                                  },
                                  "end": {
                                    "_type": "LineColumn",
                                    "line": 14,
                                    "column": 27
                                  }
                                }
                              },
                              "_type": "Punctuated",
                              "length": 1
                            },
                            "span": {
                              "_type": "Span",
                              "start": {
                                "_type": "LineColumn",
                                "line": 14,
                                "column": 22
                              },
                              "end": {
                                "_type": "LineColumn",
                                "line": 14,
                                "column": 27
                              }
                            }
                          },
                          "span": {
                            "_type": "Span",
                            "start": {
                              "_type": "LineColumn",
                              "line": 14,
                              "column": 22
                            },
                            "end": {
                              "_type": "LineColumn",
                              "line": 14,
                              "column": 27
                            }
                          }
                        },
                        "_type": "Punctuated",
                        "length": 1
                      },
                      "gt_token": {
                        "_type": "Gt",
                        "span": {
                          "_type": "Span",
                          "start": {
                            "_type": "LineColumn",
                            "line": 14,
                            "column": 27
                          },
                          "end": {
                            "_type": "LineColumn",
                            "line": 14,
                            "column": 28
                          }
                        }
                      },
                      "span": {
                        "_type": "Span",
                        "start": {
                          "_type": "LineColumn",
                          "line": 14,
                          "column": 21
                        },
                        "end": {
                          "_type": "LineColumn",
                          "line": 14,
                          "column": 28
                        }
                      }
                    },
                    "span": {
                      "_type": "Span",
                      "start": {
                        "_type": "LineColumn",
                        "line": 14,
                        "column": 15
                      },
                      "end": {
                        "_type": "LineColumn",
                        "line": 14,
                        "column": 28
                      }
                    }
                  },
                  "_type": "Punctuated",
                  "length": 1
                },
                "span": {
                  "_type": "Span",
                  "start": {
                    "_type": "LineColumn",
                    "line": 14,
                    "column": 15
                  },
                  "end": {
                    "_type": "LineColumn",
                    "line": 14,
                    "column": 28
                  }
                }
              },
              "span": {
                "_type": "Span",
                "start": {
                  "_type": "LineColumn",
                  "line": 14,
                  "column": 15
                },
                "end": {
                  "_type": "LineColumn",
                  "line": 14,
                  "column": 28
                }
              }
            },
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 13,
                "column": 4
              },
              "end": {
                "_type": "LineColumn",
                "line": 14,
                "column": 28
              }
            }
          },
          "11": {
            "_type": "Comma",
            "span": {
              "_type": "Span",
              "start": {
                "_type": "LineColumn",
                "line": 14,
                "column": 28
              },
              "end": {
                "_type": "LineColumn",
                "line": 14,
                "column": 29
              }
            }
          },
          "_type": "Punctuated",
          "length": 12
        },
        "brace_token": {
          "_type": "Brace",
          "span": {
            "_type": "Span",
            "start": {
              "_type": "LineColumn",
              "line": 2,
              "column": 17
            },
            "end": {
              "_type": "LineColumn",
              "line": 15,
              "column": 1
            }
          }
        },
        "span": {
          "_type": "Span",
          "start": {
            "_type": "LineColumn",
            "line": 2,
            "column": 17
          },
          "end": {
            "_type": "LineColumn",
            "line": 15,
            "column": 1
          }
        }
      },
      "span": {
        "_type": "Span",
        "start": {
          "_type": "LineColumn",
          "line": 1,
          "column": 0
        },
        "end": {
          "_type": "LineColumn",
          "line": 15,
          "column": 1
        }
      }
    }
  ],
  "span": {
    "_type": "Span",
    "start": {
      "_type": "LineColumn",
      "line": 1,
      "column": 0
    },
    "end": {
      "_type": "LineColumn",
      "line": 15,
      "column": 1
    }
  }
};

module.exports = {srcText: srcText, json:json};
