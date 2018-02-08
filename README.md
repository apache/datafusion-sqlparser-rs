# datafusion-sql

DataFusion SQL Parser (v2)

Goals:

- Support for custom SQL dialects, so other projects can implement their own parsers easily
- Zero-copy of tokens when parsing
- Good error reporting (e.g. show line / column numbers and descriptive messages)

