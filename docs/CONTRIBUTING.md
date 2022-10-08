# Contributing

First, thank you for contributing to Vector! The goal of this document is to
provide everything you need to start contributing to Vector. The
following TOC is sorted progressively, starting with the basics and
expanding into more specifics. Everyone from a first time contributor to a
Vector team member will find this document useful.

<!-- MarkdownTOC autolink="true" style="ordered" indent="   " -->

1. [Introduction](#introduction)
1. [Your First Contribution](#your-first-contribution)
   1. [New sources, sinks, and transforms](#new-sources-sinks-and-transforms)
1. [Workflow](#workflow)
   1. [Git Branches](#git-branches)
   1. [Git Commits](#git-commits)
      1. [Style](#style)
      1. [Signing-off](#signing-off)
   1. [Github Pull Requests](#github-pull-requests)
      1. [Title](#title)
      1. [Reviews & Approvals](#reviews--approvals)
      1. [Merge Style](#merge-style)
   1. [CI](#ci)
      1. [Releasing](#releasing)
      1. [Testing](#testing)
         1. [Skipping tests](#skipping-tests)
         1. [Daily tests](#daily-tests)
      1. [Flakey tests](#flakey-tests)
         1. [Test harness](#test-harness)
   1. [Deprecations](#deprecations)
1. [Next steps](#next-steps)
1. [Legal](#legal)
   1. [CLA](#contributor-license-agreement)
   1. [Granted rights and copyright assignment](#granted-rights-and-copyright-assignment)

<!-- /MarkdownTOC -->

## Introduction

1. **You're familiar with [Github](https://github.com) and the pull request
   workflow.**
2. **You've read Vector's [docs](https://vector.dev/docs/).**
3. **You know about the [Vector community](https://vector.dev/community/).
   Please use this for help.**

## Your First Contribution

1. Ensure your change has an issue! Find an
   [existing issue][urls.existing_issues] or [open a new issue][urls.new_issue].
   - This is where you can get a feel if the change will be accepted or not.
     Changes that are questionable will have a `needs: approval` label.
2. Once approved, [fork the Vector repository][urls.fork_repo] in your own
   Github account (only applicable to outside contributors).
3. [Create a new Git branch][urls.create_branch].
4. Review the Vector [change control](#change-control) and [development](#development) workflows.
5. Make your changes.
6. [Submit the branch as a pull request][urls.submit_pr] to the main Vector
   repo. A Vector team member should comment and/or review your pull request
   within a few days. Although, depending on the circumstances, it may take
   longer.

### New sources, sinks, and transforms

If you're contributing a new source, sink, or transform to Vector, thank you that's way cool! There's a few steps you need think about if you want to make sure we can merge your contribution. We're here to help you along with these steps but they are a blocker to getting a new integration released.

To merge a new source, sink, or transform, you need to:

- [ ] Add tests, especially integration tests if your contribution connects to an external service.
- [ ] Add instrumentation so folks using your integration can get insight into how it's working and performing. You can see some [example of instrumentation in existing integrations](https://github.com/vectordotdev/vector/tree/master/src/internal_events).
- [ ] Add documentation. You can see [examples in the `docs` directory](https://github.com/vectordotdev/vector/blob/master/docs).

## Workflow

### Git Branches

_All_ changes must be made in a branch and submitted as [pull requests](#pull-requests).
Vector does not adopt any type of branch naming style, but please use something
descriptive of your changes.

### Git Commits

#### Style

Please ensure your commits are small and focused; they should tell a story of
your change. This helps reviewers to follow your changes, especially for more
complex changes.

### Github Pull Requests

Once your changes are ready you must submit your branch as a [pull \
request](https://github.com/vectordotdev/vector/pulls).

#### Title

The pull request title must follow the format outlined in the [conventional \
commits spec](https://www.conventionalcommits.org).
[Conventional commits](https://www.conventionalcommits.org) is a standardized
format for commit messages. Vector only requires this format for commits on
the `master` branch. And because Vector squashes commits before merging
branches, this means that only the pull request title must conform to this
format. Vector performs a pull request check to verify the pull request title
in case you forget.

A list of allowed sub-categories is defined
[here](https://github.com/vectordotdev/vector/tree/master/.github).

The following are all good examples of pull request titles:

```text
feat(new sink): new `xyz` sink
feat(tcp source): add foo bar baz feature
fix(tcp source): fix foo bar baz bug
chore: improve build process
docs: fix typos
```

#### Reviews & Approvals

All pull requests should be reviewed by:

- No review required for cosmetic changes like whitespace, typos, and spelling
  by a maintainer
- One Vector team member for minor changes or trivial changes from contributors
- Two Vector team members for major changes
- Three Vector team members for RFCs

If there are any reviewers assigned, you should also wait for
their review.

#### Merge Style

All pull requests are squashed and merged. We generally discourage large pull
requests that are over 300-500 lines of diff. If you would like to propose a
change that is larger we suggest coming onto our [Discord server](https://chat.vector.dev/) and discuss it
with one of our engineers. This way we can talk through the solution and
discuss if a change that large is even needed! This will produce a quicker
response to the change and likely produce code that aligns better with our
process.

### CI

Currently Vector uses Github Actions to run tests. The workflows are defined in
`.github/workflows`.

#### Releasing

Github Actions is responsible for releasing updated versions of Vector through
various channels.

#### Testing

##### Skipping tests

Tests are run for all changes except those that have the label:

```text
ci-condition: skip
```

##### Daily tests

Some long running tests are only run daily, rather than on every pull request.
If needed, an administrator can kick off these tests manually via the button on
the [nightly build action
page](https://github.com/vectordotdev/vector/actions?query=workflow%3Anightly)

#### Flakey tests

Historically, we've had some trouble with tests being flakey. If your PR does
not have passing tests:

- Ensure that the test failures are unrelated to your change
  - Is it failing on master?
  - Does it fail if you rerun CI?
  - Can you reproduce locally?
- Find or open an issue for the test failure
  ([example](https://github.com/vectordotdev/vector/issues/3781))
- Link the PR in the issue for the failing test so that there are more examples

##### Test harness

You can invoke the [test harness][urls.vector_test_harness] by commenting on
any pull request with:

```bash
/test -t <name>
```

### Deprecations

When deprecating functionality in Vector, see [DEPRECATION.md](DEPRECATION.md).

## Next steps

As discussed in the [`README`](README.md), you should continue to the following
documents:

2. **[DEVELOPING.md](DEVELOPING.md)** - Everything necessary to develop
3. **[DOCUMENTING.md](DOCUMENTING.md)** - Preparing your change for Vector users
3. **[DEPRECATION.md](DEPRECATION.md)** - Deprecating functionality in Vector

## Legal

To protect all users of Vector, the following legal requirements are made.
If you have additional questions, please [contact us](#contact).

### Contributor License Agreement

Vector requires all contributors to sign the a Contributor License Agreement
(CLA). This gives Vector the right to use your contribution as well as ensuring
that you own your contributions and can use them for other purposes.

The full text of the CLA can be found at [https://cla.datadoghq.com/vectordotdev/vector](https://cla.datadoghq.com/vectordotdev/vector).

### Granted rights and copyright assignment

This is covered by the CLA.

[urls.create_branch]: https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository
[urls.existing_issues]: https://github.com/vectordotdev/vector/issues
[urls.fork_repo]: https://help.github.com/en/github/getting-started-with-github/fork-a-repo
[urls.new_issue]: https://github.com/vectordotdev/vector/issues/new
[urls.submit_pr]: https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork
[urls.vector_test_harness]: https://github.com/vectordotdev/vector-test-harness/
