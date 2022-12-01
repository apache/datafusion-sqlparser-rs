# Writing a Custom SQL Parser

I have explored many different ways of building this library to make it easy to extend it for custom SQL dialects. Most of my attempts ended in failure but I have now found a workable solution. It is not without downsides but this seems to be the most pragmatic solution.

The concept is simply to write a new parser that delegates to the ANSI parser so that as much as possible of the core functionality can be re-used.

I also plan on building in specific support for custom data types, where a lambda function can be passed to the parser to parse data types.

For an example of this, see the [DataFusion](https://github.com/apache/arrow-datafusion) project and its [query planner] (https://github.com/apache/arrow-datafusion/tree/master/datafusion/sql).

