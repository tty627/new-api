#!/usr/bin/env bash
# FORK-04 migration matrix against one database, using real binaries:
#   1 upstream fresh install  2 fork upgrade  3 fork restart  4 upstream on fork DB
# Usage: fork-migration-matrix.sh <upstream-binary> <fork-binary>
# Database is chosen by SQL_DSN (empty = SQLite at $SQLITE_PATH), as for the server.
# QUERY must be a command that runs one SQL statement against that database and
# prints the first column of the first row.
set -euo pipefail
UP=$1 FORK=$2
PORT=${MATRIX_PORT:-39200}
: "${QUERY:?set QUERY to a command that runs SQL}"
q() { $QUERY "$1"; }

run() {
  local bin=$1 label=$2 version=""
  "$bin" --port "$PORT" > "$label.log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 120); do
    version=$(curl -s -m 2 "http://127.0.0.1:$PORT/api/status" | grep -o '"version":"[^"]*"' | cut -d'"' -f4) || true
    [ -n "$version" ] && break
    sleep 1
  done
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  if [ -z "$version" ]; then
    echo "::error::$label did not start"; tail -30 "$label.log"; exit 1
  fi
  echo "$label: ok ($version)"
}
expect() { # expect <label> <actual> <wanted>
  if [ "$2" != "$3" ]; then echo "::error::$1: got '$2', want '$3'"; exit 1; fi
  echo "  $1: $2"
}
table_count() { q "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'fork_subscription_order_links'"; }

run "$UP" 1-upstream-fresh
q "INSERT INTO subscription_plans (title, price_amount, currency, duration_unit, duration_value, enabled, total_amount) VALUES ('mig', 9.9, 'CNY', 'month', 1, true, 100)"
q "INSERT INTO subscription_orders (user_id, plan_id, money, trade_no, payment_method, payment_provider, status, create_time) VALUES (1, 1, 9.9, 'MIGOLD', 'alipay', 'epay', 'pending', 1)"
expect "fork table before upgrade" "$(table_count)" 0
run "$FORK" 2-fork-upgrade
expect "fork table after upgrade" "$(table_count)" 1
expect "pre-fork pending order kept" "$(q "SELECT COUNT(*) FROM subscription_orders WHERE trade_no = 'MIGOLD'")" 1
run "$FORK" 3-fork-restart
run "$UP" 4-upstream-on-fork-db
expect "fork table kept after upstream boot" "$(table_count)" 1
