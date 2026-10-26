#!/usr/bin/env bash
# FORK-04 migration matrix against one database, using real binaries:
#   fresh: fork install, restart, upstream rollback compatibility
#   upgrade: upstream install, fork upgrade, restart, upstream rollback compatibility
# Usage: fork-migration-matrix.sh <upstream-binary> <fork-binary>
# MATRIX_DB is sqlite/mysql/postgres; MATRIX_SCENARIO is fresh/upgrade.
# Use an empty, isolated database. SQL_DSN/SQLITE_PATH select the server database.
# QUERY must be a command that runs one SQL statement against that database and
# fails on SQL errors, printing the first column of the first row on success.
set -euo pipefail
UP=$1 FORK=$2
PORT=${MATRIX_PORT:-39200}
: "${QUERY:?set QUERY to a command that runs SQL}"
: "${MATRIX_DB:?set MATRIX_DB to sqlite, mysql or postgres}"
SCENARIO=${MATRIX_SCENARIO:-upgrade}
case "$MATRIX_DB" in sqlite|mysql|postgres) ;; *) echo "invalid MATRIX_DB"; exit 2 ;; esac
case "$SCENARIO" in fresh|upgrade) ;; *) echo "invalid MATRIX_SCENARIO"; exit 2 ;; esac
q() { $QUERY "$1"; }
pid=""
cleanup() {
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    pid=""
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

run() {
  local bin=$1 label=$2 version=""
  "$bin" --port "$PORT" > "$label.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 120); do
    kill -0 "$pid" 2>/dev/null || break
    version=$(curl -s -m 2 "http://127.0.0.1:$PORT/api/status" | grep -o '"version":"[^"]*"' | cut -d'"' -f4) || true
    [ -n "$version" ] && break
    sleep 1
  done
  cleanup
  if [ -z "$version" ]; then
    echo "::error::$label did not start"; tail -30 "$label.log"; exit 1
  fi
  echo "$label: ok ($version)"
  if [ "$bin" = "$FORK" ] && [ -n "${EXPECTED_FORK_VERSION:-}" ]; then
    expect "fork status version" "$version" "$EXPECTED_FORK_VERSION"
  fi
}
expect() { # expect <label> <actual> <wanted>
  if [ "$2" != "$3" ]; then echo "::error::$1: got '$2', want '$3'"; exit 1; fi
  echo "  $1: $2"
}
table_count() {
  local name=${1:-fork_subscription_order_links}
  case "$MATRIX_DB" in
    sqlite) q "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '$name'" ;;
    mysql) q "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '$name'" ;;
    postgres) q "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = '$name'" ;;
  esac
}
case "$MATRIX_DB" in
  sqlite) tables=$(q "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'") ;;
  mysql) tables=$(q "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()") ;;
  postgres) tables=$(q "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema()") ;;
esac
expect "isolated database starts empty" "$tables" 0
if curl -fsS -m 2 "http://127.0.0.1:$PORT/api/status" >/dev/null 2>&1; then
  echo "::error::matrix port is already serving an API"; exit 1
fi

if [ "$SCENARIO" = upgrade ]; then
  run "$UP" 1-upstream-fresh
  expect "fork table before upgrade" "$(table_count)" 0
else
  run "$FORK" 1-fork-fresh
fi
q "INSERT INTO subscription_plans (title, price_amount, currency, duration_unit, duration_value, enabled, total_amount) VALUES ('mig', 9.9, 'CNY', 'month', 1, true, 100)"
q "INSERT INTO subscription_orders (user_id, plan_id, money, trade_no, payment_method, payment_provider, status, create_time) VALUES (1, 1, 9.9, 'MIGOLD', 'alipay', 'epay', 'pending', 1)"
if [ "$SCENARIO" = upgrade ]; then
  run "$FORK" 2-fork-upgrade
fi
expect "fork table installed" "$(table_count)" 1
expect "pending order kept" "$(q "SELECT COUNT(*) FROM subscription_orders WHERE trade_no = 'MIGOLD' AND status = 'pending' AND money = 9.9")" 1

# Schema-only sentinels exercise each independent unique key, including the
# multiple-NULL behaviour needed for pending orders. These are not paid orders.
q "INSERT INTO fork_subscription_order_links (order_id, trade_no, plan_id, snapshot, snapshot_source, user_subscription_id) VALUES (91001, 'MIGLINK1', 1, '{}', 'order', 91001), (91002, 'MIGLINK2', 1, '{}', 'order', NULL), (91003, 'MIGLINK3', 1, '{}', 'order', NULL)"
reject() {
  if q "$2" > constraint.log 2>&1; then
    echo "::error::$1 accepted a duplicate"; exit 1
  fi
  echo "  $1: duplicate rejected"
}
reject "order_id unique" "INSERT INTO fork_subscription_order_links (order_id, trade_no, plan_id, snapshot_source, user_subscription_id) VALUES (91001, 'MIGDUP1', 1, 'order', 92001)"
reject "trade_no unique" "INSERT INTO fork_subscription_order_links (order_id, trade_no, plan_id, snapshot_source, user_subscription_id) VALUES (92002, 'MIGLINK1', 1, 'order', 92002)"
reject "subscription unique" "INSERT INTO fork_subscription_order_links (order_id, trade_no, plan_id, snapshot_source, user_subscription_id) VALUES (92003, 'MIGDUP3', 1, 'order', 91001)"
expect "multiple pending NULL links" "$(q "SELECT COUNT(*) FROM fork_subscription_order_links WHERE user_subscription_id IS NULL")" 2
run "$FORK" 3-fork-restart
expect "link rows kept on restart" "$(q "SELECT COUNT(*) FROM fork_subscription_order_links")" 3
reject "subscription still unique after restart" "INSERT INTO fork_subscription_order_links (order_id, trade_no, plan_id, snapshot_source, user_subscription_id) VALUES (92004, 'MIGDUP4', 1, 'order', 91001)"
run "$UP" 4-upstream-on-fork-db
expect "fork table kept after upstream boot" "$(table_count)" 1
expect "link rows kept after upstream boot" "$(q "SELECT COUNT(*) FROM fork_subscription_order_links")" 3
expect "order kept after upstream boot" "$(q "SELECT COUNT(*) FROM subscription_orders WHERE trade_no = 'MIGOLD' AND status = 'pending' AND money = 9.9")" 1
