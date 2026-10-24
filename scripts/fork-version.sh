#!/bin/sh
# Prints the fork build version: <FORK_VERSION>+up.<upstream short commit> (FORK-05).
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
fork=$(tr -d ' \r\n' < "$root/FORK_VERSION")
up=$(tr -d ' \r\n' < "$root/UPSTREAM_COMMIT" | cut -c1-9)
printf '%s+up.%s\n' "$fork" "$up"
