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


## Process Overview

As part of the Apache governance model, official releases consist of signed
source tarballs approved by the DataFusion PMC.

We then use the code in the approved artifacts to release to crates.io.

### Change Log

We maintain a `CHANGELOG.md` so our users know what has been changed between releases.

You will need a GitHub Personal Access Token for the following steps. Follow
[these instructions](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
to generate one if you do not already have one.

The changelog is generated using a Python script which needs `PyGitHub`, installed using pip:

```shell
pip3 install PyGitHub
```

To generate the changelog, set the `GITHUB_TOKEN` environment variable to a valid token and then run the script
providing two commit ids or tags followed by the version number of the release being created. The following
example generates a change log of all changes between the first commit and the current HEAD revision.

```shell
export GITHUB_TOKEN=<your-token-here>
python ./dev/release/generate-changelog.py v0.51.0 HEAD 0.52.0 > changelog/0.52.0.md
```

This script creates a changelog from GitHub PRs based on the labels associated with them as well as looking for
titles starting with `feat:`, `fix:`, or `docs:`.

Add an entry to CHANGELOG.md for the new version 

## Prepare release commits and PR

### Update Version

Checkout the main commit to be released

```shell
git fetch apache
git checkout apache/main
```

Manually update the version in the root `Cargo.toml` to the release version (e.g. `0.52.0`).

Lastly commit the version change:

```shell
git commit -a -m 'Update version'
```

## Prepare release candidate artifacts

After the PR gets merged, you are ready to create release artifacts from the
merged commit.

(Note you need to be a committer to run these scripts as they upload to the apache svn distribution servers)

### Pick a Release Candidate (RC) number

Pick numbers in sequential order, with `0` for `rc0`, `1` for `rc1`, etc.

### Create git tag for the release:

While the official release artifacts are signed tarballs and zip files, we also
tag the commit it was created for convenience and code archaeology.

Using a string such as `v0.52.0` as the `<version>`, create and push the tag by running these commands:

```shell
git fetch apache
git tag <version> apache/main
# push tag to Github remote
git push apache <version>
```

### Create, sign, and upload artifacts

Run `create-tarball.sh` with the `<version>` tag and `<rc>` and you found in previous steps:

```shell
GITHUB_TOKEN=<TOKEN> ./dev/release/create-tarball.sh 0.52.0 1
```

The `create-tarball.sh` script

1. creates and uploads all release candidate artifacts to the [datafusion
   dev](https://dist.apache.org/repos/dist/dev/datafusion) location on the
   apache distribution svn server

2. provide you an email template to
   send to dev@datafusion.apache.org for release voting.

### Vote on Release Candidate artifacts

Send the email output from the script to dev@datafusion.apache.org.

For the release to become "official" it needs at least three PMC members to vote +1 on it.

### Verifying Release Candidates

The `dev/release/verify-release-candidate.sh` is a script in this repository that can assist in the verification process. Run it like:

```shell
./dev/release/verify-release-candidate.sh 0.52.0 1
```

#### If the release is not approved

If the release is not approved, fix whatever the problem is, merge changelog
changes into main if there is any and try again with the next RC number.

## Finalize the release

NOTE: steps in this section can only be done by PMC members.

### After the release is approved

Move artifacts to the release location in SVN, using the `release-tarball.sh` script:

```shell
./dev/release/release-tarball.sh 0.52.0 1
```

Congratulations! The release is now official!

### Publish on Crates.io

Only approved releases of the tarball should be published to
crates.io, in order to conform to Apache Software Foundation
governance standards.

A DataFusion committer can publish this crate after an official project release has
been made to crates.io using the following instructions.

Follow [these
instructions](https://doc.rust-lang.org/cargo/reference/publishing.html) to
create an account and login to crates.io before asking to be added as an owner
to the sqlparser DataFusion crates.

Download and unpack the official release tarball

Verify that the Cargo.toml in the tarball contains the correct version
(e.g. `version = "0.52.0"`) and then publish the crates by running the following commands

```shell
(cd sqlparser && cargo publish)
```

If necessary, also publish the `sqlparser_derive` crate:

crates.io homepage: https://crates.io/crates/sqlparser_derive

```shell
(cd derive && cargo publish
```
