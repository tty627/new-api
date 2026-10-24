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
| P2 | `model/subscription.go` · `SubscriptionOrder.Insert` | When linking is enabled, insert order + plan snapshot in one transaction (`forkInsertOrderWithSnapshot`). Covers all four payment controllers. | LINK-01 | Yes | active |
| P3 | `model/subscription.go` · `CompleteSubscriptionOrder` | After loading the plan, `forkPlanForOrderTx` swaps in the order snapshot; after `CreateUserSubscriptionFromPlanTx`, `forkLinkOrderTx` records the link in the same transaction | LINK-01, LINK-02 | Yes | active |
| P4 | `model/subscription.go` · `PurchaseSubscriptionWithBalance` | After creating the order, `forkLinkBalanceOrderTx` writes snapshot + link | LINK-02 | Yes | active |
| P5 | `model/main.go` · `migrateDB` AutoMigrate list | Append `&ForkSubscriptionOrderLink{}` | FORK-04 | Yes | active |
| P6 | `router/api-router.go` · `subscriptionAdminRoute` | `GET /order-links/:trade_no` → `AdminGetSubscriptionOrderLink` | LINK-04 | Yes | active |
| P7 | `middleware/access_token_routes.go` · route rule table | Declare the new route with `billing:read` (required by `TestAccessTokenRouteRulesCoverEveryDashboardRoute`) | LINK-04 | Yes | active |
| P8 | `main.go` · `main` | `link-backfill` subcommand dispatch, next to the upstream `plugin` subcommand | LINK-03 | No | active |

P2–P7 are one upstreamable unit (subscription order snapshot + entitlement link). `FORK_SUBSCRIPTION_LINK_ENABLED=false` turns P2–P4 back into upstream behaviour without a rebuild.

## Fork-owned files

- `PATCHES.md`, `FORK_VERSION`, `UPSTREAM_COMMIT`
- `scripts/fork-version.sh`
- `.github/workflows/fork-guard.yml`
- `common/version_fork_test.go`
- `model/subscription_link_fork.go`, `model/subscription_link_fork_test.go` (table `fork_subscription_order_links`)
- `controller/subscription_link_fork.go`
- `subscription_link_backfill_fork.go` (`new-api link-backfill [--apply] [--window 300]`)
- `scripts/fork-migration-matrix.sh`
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
