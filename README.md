# datafusion-sql

This is a work-in-progress to develop a new version of the DataFusion SQL Parser.

Goals for this version:

- Support for custom SQL dialects, so other projects can implement their own parsers easily
- Good error reporting (e.g. show line / column numbers and descriptive messages)
- Zero-copy of tokens when parsing
- Concise code


