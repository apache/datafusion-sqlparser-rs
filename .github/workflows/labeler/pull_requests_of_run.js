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

// Used by the dialect labeler when it runs on `workflow_run`.
//
// `workflow_run.pull_requests` is empty for pull requests from forks, so find
// the open pull requests of the run's head branch. They are the current ones,
// which can be newer than the run's commit when pushes follow each other.
// The same branch can target several bases, so return every match.
module.exports = async ({ github, context }) => {
  const run = context.payload.workflow_run;
  // The head repository is null when the fork was deleted.
  if (!run.head_repository) {
    return [];
  }
  const { data } = await github.rest.pulls.list({
    owner: context.repo.owner,
    repo: context.repo.repo,
    state: "open",
    head: `${run.head_repository.owner.login}:${run.head_branch}`,
  });
  return data;
};
