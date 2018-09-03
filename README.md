# SQL Parser

The goal of this project is to build a SQL lexer and parser capable of parsing ANSI SQL:2011 (or 2016 if I can get access to the specification for free).

The current code was copied from DataFusion and has some non-standard SQL support and only a subset of ANSI SQL currently.

The current code is capable of parsing some simple SELECT and CREATE TABLE statements.