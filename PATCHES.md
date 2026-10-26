# Fork patches

This fork is `tty627/new-api`, based on [QuantumNous/new-api](https://github.com/QuantumNous/new-api).
The upstream base commit is recorded in `UPSTREAM_COMMIT`; the fork version in `FORK_VERSION`.
Requirement IDs refer to the OpenSpec changes in the separate spec repository: `maintain-new-api-fork` (FORK/DEP/LINK), `harden-platform-billing` (BILL/CYCLE), and `specify-platform-core` (platform behavior and release acceptance).

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
| P5 | `model/main.go` · `migrateDB` AutoMigrate list | Append link table, durable billing sessions and append-only events | FORK-04, BILL-01 | Yes | active |
| P6 | `router/api-router.go` · `subscriptionAdminRoute` | `GET /order-links/:trade_no` → `AdminGetSubscriptionOrderLink` | LINK-04 | Yes | active |
| P7 | `middleware/access_token_routes.go` · route rule table | Declare the new route with `billing:read` (required by `TestAccessTokenRouteRulesCoverEveryDashboardRoute`) | LINK-04 | Yes | active |
| P8 | `main.go` · `main`, `InitResources` | Link/backfill and billing recovery CLI; bounded recovery worker; reject legacy batch writes before DB initialization; optional BIND_HOST; pprof restricted to loopback | LINK-03, BILL-04, BILL-05, BILL-06, OPS-04 | No | active |
| P9 | `service/channel_affinity_usage_cache_test.go` · cache statistics fixtures | Use each test's name and reset its own cache entry, avoiding Windows clock-resolution collisions between tests | FORK-03 | Yes | active |
| P10 | `model/subscription.go` · subscription/record schema, reserve/refund/reset/cleanup and administration | Snapshot a nullable cycle identity, advance generation on all resets, delegate request-aware mutations to fork Tx APIs, retain unresolved records and lock users before subscriptions during cancellation/deletion/expiry | CYCLE-01, CYCLE-02, CYCLE-03, CYCLE-04 | Yes | active |
| P11 | `service/billing_session.go` · session factory, reserve/settle/refund | Factory-created sessions use durable atomic ledger operations; retain existing interface | BILL-01, BILL-02, BILL-03 | Yes | active |
| P12 | `service/quota.go` · legacy subscription settlement | Require request identity for subscription adjustments instead of raw cross-cycle deltas | CYCLE-04 | Yes | active |
| P13 | `service/task_billing.go` · acceptance statistics, refund/recalculation | Use durable task accounting and terminal intent, prevent independent wallet/token updates and duplicate statistics | BILL-03, BILL-04, CYCLE-04 | Yes | active |
| P14 | `service/task_polling.go` · timeout sweep and terminal completion | Keep unknown timed-out reservations; persist terminal billing evidence and close zero-difference sessions | BILL-04, BILL-05, TASK-02 | Yes | active |
| P15 | `service/violation_fee.go` · fee charging | Use a separate stable durable billing identity for explicit policy fees | BILL-01, BILL-02 | Yes | active |
| P16 | `service/relay_error.go` · `DecideRelayRetry` | Stop automatic replay after an unknown transport/read outcome | BILL-05, ROUTE-03 | Yes | active |
| P17 | `controller/relay.go` · task submission lifecycle | Preserve outcome uncertainty; insert accepted tasks, bind accounting and persist acceptance intent atomically before awaiting actual terminal completion | BILL-03, BILL-05, TASK-02 | Yes | active |
| P18 | `relay/request_billing.go` · final error handling | Pass the final error classification to the refund decision | BILL-05 | Yes | active |
| P19 | `controller/topup.go` · Epay operational logs | Exclude signed query strings, payment parameters and full verification payloads from logs | OPS-04 | Yes | active |
| P20 | `middleware/logger.go` · access log formatter | Omit Epay callback/return query strings while preserving the handler's original request | OPS-04 | Yes | active |
| P21 | `controller/payment_webhook_availability_test.go` · Epay cases | Assert signed callbacks never leak their signature into operational logs | OPS-04 | Yes | active |
| P22 | `middleware/logger_test.go` · query logging cases | Assert Epay query redaction without changing the request consumed by the handler | OPS-04 | Yes | active |
| P23 | `service/task_billing_test.go` · task fixtures and accounting cases | Migrate billing tables, use persisted task identities and assert safe legacy behavior | BILL-01, BILL-04, CYCLE-04 | Yes | active |
| P24 | `service/text_quota_test.go` · DB fixture | Migrate durable billing tables used by factory-created billing sessions | BILL-01 | Yes | active |
| P25 | `service/relay_error_test.go` · retry policy cases | Assert uncertain upstream outcomes do not replay even with retry budget | BILL-05 | Yes | active |
| P26 | `service/task_polling_test.go` · timeout regression | Preserve both old and new unknown task quotas and avoid fabricated refunds | BILL-05, TASK-02 | Yes | active |
| P27 | `relay/relay_task_test.go` · failed task fixture | Seed a modern task ledger and its charged usage for the refund regression | BILL-04 | Yes | active |
| P28 | `controller/responses_websocket_test.go` · DB fixture | Migrate ledger tables used by real WebSocket billing | BILL-01 | Yes | active |
| P29 | `controller/relay_task_plugin_test.go` · dialect fixture | Include ledger tables and consistent context/session request identities for real task submission contracts | BILL-01, TASK-02 | Yes | active |
| P30 | `controller/plugin_native_e2e_test.go` · task fixture | Use the real durable billing session for native task submission and settlement | BILL-01, TASK-02 | Yes | active |
| P31 | `controller/plugin_protocol_test.go` · terminal fixture | Seed the previously charged user and ledger tables for disconnect-during-settlement regression | BILL-04, TASK-02 | Yes | active |
| P32 | `service/funding_source.go` · subscription source | Resolve adjustments against the original request and cycle instead of unrestricted deltas | CYCLE-02, CYCLE-04 | Yes | active |
| P33 | `controller/billing_option_test.go` · policy fixture | Include ledger tables used by real pre-consumption policy tests | BILL-01 | Yes | active |
| P34 | `e2e/doc_parse_test.go` · document task fixture | Include persisted owner and ledger tables for real batch completion | BILL-04, TASK-02 | Yes | active |
| P35 | `controller/access_token_audit_test.go` · SQLite fixture | Close the original SQLite connection even when callers reopen the database; allow Windows cleanup | REL-02 | Yes | active |
| P36 | `controller/user.go` · failed redemption log | Exclude the usable redemption code from failure logs | OPS-04 | Yes | active |
| P37 | `common/log_text.zh-CN.json` · redemption log translation | Match the redacted log without a redemption-code argument | OPS-04 | Yes | active |
| P38 | `relay/channel/openai/relay-openai.go` · chat stream completion | Treat a truncated stream without terminal evidence or provider usage as unknown; retain its reservation and prevent replay | BILL-05, RELAY-02 | Yes | active |
| P39 | `relay/channel/openai/stream_tool_billing_test.go` · stream completion regressions | Reproduce truncated EOF and retain compatibility for explicit finish, usage and DONE | BILL-05, RELAY-02 | Yes | active |
| P40 | `model/midjourney.go` · task schema | Persist the private billing request identity used for atomic task accounting | BILL-01, BILL-03, TASK-02 | Yes | active |
| P41 | `relay/mjproxy_handler.go` · submit and swap-face handlers | Reserve wallet and token before upstream calls; atomically persist accepted task and billing intent; return an explicit verification error for uncertain outcomes while retaining the reservation | BILL-01, BILL-03, BILL-05, TASK-02 | Yes | active |
| P42 | `controller/midjourney.go` · polling and completion | Persist terminal evidence with billing intent, retain unknown reservations and handle separate local requests sharing an upstream task ID | BILL-03, BILL-04, BILL-05, TASK-02 | Yes | active |
| P43 | `service/midjourney.go` · legacy settlement entry | Reject unproven direct settlement; route active callers through request-aware durable accounting | BILL-04, BILL-05 | Yes | active |

P2–P7 are one upstreamable unit (subscription order snapshot + entitlement link). `FORK_SUBSCRIPTION_LINK_ENABLED=false` turns P2–P4 back into upstream behaviour without a rebuild.

## Fork-owned files

- `PATCHES.md`, `FORK_VERSION`, `UPSTREAM_COMMIT`
- `scripts/fork-version.sh`
- `.github/workflows/fork-guard.yml`
- `common/version_fork_test.go`
- `model/subscription_link_fork.go`, `model/subscription_link_fork_test.go` (table `fork_subscription_order_links`)
- `controller/subscription_link_fork.go`
- `subscription_link_backfill_fork.go`, `subscription_link_backfill_fork_test.go` (`new-api link-backfill [--apply] [--window 300]`)
- `scripts/fork-migration-matrix.sh`
- `model/billing_session_fork.go`, `service/billing_recovery_fork.go`, `service/billing_recovery_fork_test.go`
- `model/billing_midjourney_fork.go`, `service/midjourney_billing_fork.go`
- `model/subscription_cycle_fork.go`, `model/subscription_cycle_fork_test.go`
- `runtime_fork.go`, `billing_recovery_fork.go`
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
`testdata/bridge_delivery.json` is generated by the Python bridge's notification
formatter using a dummy merchant key. The contract tests pass that unchanged
payload to the real top-up and subscription handlers twice to cover a lost ACK.

Backfill opens an already migrated database without server initialization.
The default preview performs no migrations or writes; SQLite is opened with
`mode=ro`. It conservatively links only a single `Source=order` subscription to
a single unlinked completed order for the same user and plan within the time
window. Multiple candidates remain `unlinkable` even when their counts match.

The migration workflow runs SQLite, MySQL and PostgreSQL for both direct fork
installation and upgrade from `UPSTREAM_COMMIT`. Each job starts with an empty
isolated database, checks the order/link unique constraints and nullable pending
links, restarts the fork, and then boots the upstream binary against the same
database. It also checks the fork version returned by `/api/status`.
For a local run, set `MATRIX_DB=sqlite|mysql|postgres`,
`MATRIX_SCENARIO=fresh|upgrade`, `SQL_DSN`/`SQLITE_PATH`, and `QUERY` as documented
in `scripts/fork-migration-matrix.sh`. The SQL query command must exit nonzero on
errors. A workflow definition is not evidence that a new revision has passed.

## Sync checklist (FORK-03)

1. `git fetch upstream` and fast-forward the `upstream` branch to the chosen upstream commit.
2. From `main`, create `sync/<upstream short sha>` and merge `upstream`.
3. Re-check every `active` row above; re-apply hooks where upstream moved code.
4. Diff the `model.SubscriptionPlan` struct against the previous upstream base. If fields changed, update the LINK snapshot.
5. Run `make test`, `go test ./test/contract/... -count=1`, and the SQLite / MySQL / PostgreSQL migration matrix.
6. Update `UPSTREAM_COMMIT`, bump `FORK_VERSION` if needed, merge into `main`, tag `fork-vX.Y.Z`.
