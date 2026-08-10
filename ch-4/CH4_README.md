# CH-4 — CI/CD workflows and how to test them locally

The repository contains two GitHub Actions workflows.

| Workflow | When it runs | What it does |
| --- | --- | --- |
| `.github/workflows/wiki-ci.yml` | On every push and pull request for `main` and the `ch-*` branches | Lints the code, runs the tests, and builds the Docker image |
| `.github/workflows/wiki-cd.yml` | After CI finishes successfully on `main` | Publishes the Docker image to GHCR |

## Jobs

`wiki-ci` contains five jobs:

| Job | What it does |
| --- | --- |
| `resolveChapter` | Decides which chapter directory the run works on |
| `lint` | Runs golangci-lint |
| `unitTest` | Runs `go test -race ./...` |
| `integrationTest` | Runs the tests that need a real Cassandra node |
| `dockerBuild` | Builds the image for `linux/amd64` and `linux/arm64`, without pushing it |

`wiki-cd` contains one job:

| Job | What it does |
| --- | --- |
| `publishImage` | Logs in to GHCR and pushes the image |

All four later CI jobs depend on `resolveChapter`, so it always runs first.

## Why `resolveChapter` exists

The repository has a root folder and inner projects: `ch-1`, `ch-2`, and so on up to
`ch-10`. Each inner project is independent and can run on its own. The branches follow
the same naming pattern, so branch `ch-4` matches directory `ch-4/`.

Because of this, the workflows do not hard-code a directory. The `resolveChapter` job
works out the correct chapter first and shares it as a variable. Every other job then
reads that value:

```yaml
working-directory: ${{ needs.resolveChapter.outputs.chapter }}
```

The rule is simple:

- If the branch name looks like `ch-<number>`, the job uses that directory.
- For any other branch, including `main`, the job uses the chapter directory with the
  highest number.

This means the same workflow files will keep working for `ch-5` up to `ch-10`. You do
not need to edit them when you add a new chapter.

---

## Testing the workflows locally with act

[act](https://github.com/nektos/act) runs GitHub Actions workflows on your own machine
inside Docker containers. You can check a change in seconds instead of pushing it to
GitHub and waiting for the result.

Docker must be running before you start.

### 1. Install act

```bash
brew install act
```

### 2. Run act from the repository root

act looks for `.github/workflows/` in the current directory, so run every command from
the root of the repository, not from `ch-4/`.

### 3. Apple M-series chip

On an M-series Mac, act prints this warning:

```text
WARN ⚠ You are using Apple M-series chip and you have not specified container
architecture, you might encounter issues while running act. If so, try running it
with '--container-architecture linux/amd64'. ⚠
```

Add the flag to your commands:

```bash
act -l --container-architecture linux/amd64
```

To avoid typing it every time, save it once in a `.actrc` file in the repository root:

```bash
echo "--container-architecture linux/amd64" > .actrc
```

### 4. List the workflows

```bash
act -l
```

This shows every job, its workflow, and the events that trigger it. The **Stage**
column shows the order: `resolveChapter` is alone in stage 0, and the other CI jobs
follow in stage 1.

### 5. Check each job without running it

The `-n` flag (dry run) reads the workflow and checks that all expressions and
conditions are correct. It does not start any container, so it is very fast.

```bash
act push -n -j resolveChapter
act push -n -j lint
act push -n -j unitTest
act push -n -j dockerBuild
```

Do not dry run `integrationTest`, and do not dry run the whole `push` event. That job
uses a service container, and act (version 0.2.89) crashes when it checks the health of
a service that a dry run never created. This is a bug in act, not a problem in the
workflow.

### 6. Run the full push event

```bash
act push --concurrent-jobs 1
```

By default act starts all jobs at the same time on one machine. On GitHub each job gets
its own runner, so this is not the same situation. Jobs that run together can compete
for memory and for the Go cache, and a job can fail for that reason alone.
`--concurrent-jobs 1` runs the jobs one after another. It takes longer, but the result
is easier to trust.

---

## Testing the merge to `main`

`wiki-cd` does not start on its own. It starts only after `wiki-ci` finishes, through
the `workflow_run` event, and it publishes the image only when three conditions are
true:

```yaml
github.event.workflow_run.conclusion == 'success' &&
github.event.workflow_run.head_branch == 'main' &&
github.event.workflow_run.event == 'push'
```

act cannot create this situation by itself. It never runs CI and CD one after the
other, and it does not invent the values above. If you simply run `act workflow_run`,
all three values are empty, the condition is false, and the job is skipped without any
message. The command still finishes with success, so it looks like a passing test even
though nothing was tested.

To test the condition, you must describe the CI run yourself in a JSON file and pass it
with `-e`. These files are local helpers. They are not part of the repository, so
create them yourself in `.github/act/`.

### `workflow_run-success.json`

This file describes a **successful** CI run on `main`. All three conditions are true,
so the CD job should start.

```json
{
  "workflow_run": {
    "name": "CI (wiki)",
    "conclusion": "success",
    "head_branch": "main",
    "event": "push",
    "head_sha": "0000000000000000000000000000000000000000"
  }
}
```

### `workflow_run-rejected.json`

This file is identical except for one field: `"conclusion": "failure"`. It describes a
CI run that **failed** on `main`. The condition is false, so the CD job must not start.

```json
{
  "workflow_run": {
    "name": "CI (wiki)",
    "conclusion": "failure",
    "head_branch": "main",
    "event": "push",
    "head_sha": "0000000000000000000000000000000000000000"
  }
}
```

### Run both checks

```bash
act workflow_run -n -e .github/act/workflow_run-success.json
act workflow_run -n -e .github/act/workflow_run-rejected.json
```

What you should see:

| File | Expected result |
| --- | --- |
| `workflow_run-success.json` | The job runs and prints its steps, up to `Extract Docker metadata` |
| `workflow_run-rejected.json` | The job is skipped and prints **no** steps at all |

The second file is the more important one. A condition that allows the correct case is
easy to write; the real question is whether it also blocks the wrong case. This test
proves that a failed CI run cannot publish an image. You can also copy the file and
change `head_branch` to `ch-4` to check that a chapter branch cannot publish either.

Always keep the `-n` flag here. The `publishImage` job pushes to a real registry, and
you do not want a local test to publish an image.
