<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Writing a Custom SQL Parser

I have explored many different ways of building this library to make it easy to extend it for custom SQL dialects. Most of my attempts ended in failure but I have now found a workable solution. It is not without downsides but this seems to be the most pragmatic solution.

The concept is simply to write a new parser that delegates to the ANSI parser so that as much as possible of the core functionality can be re-used.

I also plan on building in specific support for custom data types, where a lambda function can be passed to the parser to parse data types.

For an example of this, see the [DataFusion](https://github.com/apache/arrow-datafusion) project and its [query planner](https://github.com/apache/arrow-datafusion/tree/master/datafusion/sql).

