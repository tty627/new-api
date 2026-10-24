# Fork patches

This fork is `tty627/new-api`, based on [QuantumNous/new-api](https://github.com/QuantumNous/new-api).
The upstream base commit is recorded in `UPSTREAM_COMMIT`; the fork version in `FORK_VERSION`.
Requirement IDs (FORK, DEP, LINK) refer to the `maintain-new-api-fork` OpenSpec change, kept in a separate spec repository.

Rules (FORK-02):

- Every change to a file that exists upstream needs a row below, updated in the same PR.
- New logic goes into fork-owned files (`*_fork.go`, `test/contract/`, `fork_*` tables). Upstream files only get call hooks.
- When upstream ships an equivalent fix, mark the row `retired` and keep any related contract test.

## Upstream file changes

| ID | Upstream file · function | Change | Requirement | Upstreamable? | Status |
|---|---|---|---|---|---|
| P1 | `Dockerfile` · go build `-ldflags` | `common.Version` comes from `scripts/fork-version.sh` instead of `VERSION` | FORK-05 | No | active |

## Fork-owned files

- `PATCHES.md`, `FORK_VERSION`, `UPSTREAM_COMMIT`
- `scripts/fork-version.sh`
- `.github/workflows/fork-guard.yml`
- `common/version_fork_test.go`
- `test/contract/` (DEP-01 … DEP-05 contract tests; `testdata/money_golden.json` is shared with the payment bridge)

## Running the contract tests

```sh
go test ./test/contract/ -count=1                       # SQLite only
TEST_POSTGRES_DSN='postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable' \
TEST_MYSQL_DSN='root:pass@tcp(127.0.0.1:3306)/mysql' \
go test ./test/contract/ -count=1                       # full matrix (loopback servers only)
CONTRACT_UPDATE_GOLDEN=1 go test ./test/contract/ -run DEP03   # regenerate money golden file
```

Each test creates an isolated database and drops it afterwards.

## Sync checklist (FORK-03)

1. `git fetch upstream` and fast-forward the `upstream` branch to the chosen upstream commit.
2. From `main`, create `sync/<upstream short sha>` and merge `upstream`.
3. Re-check every `active` row above; re-apply hooks where upstream moved code.
4. Diff the `model.SubscriptionPlan` struct against the previous upstream base. If fields changed, update the LINK snapshot.
5. Run `make test`, `go test ./test/contract/... -count=1`, and the SQLite / MySQL / PostgreSQL migration matrix.
6. Update `UPSTREAM_COMMIT`, bump `FORK_VERSION` if needed, merge into `main`, tag `fork-vX.Y.Z`.
