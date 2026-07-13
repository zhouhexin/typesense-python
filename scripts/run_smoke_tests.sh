#!/usr/bin/env bash
# Thin wrapper around scripts/smoke_test.py.
#
# By default uses the alias prefix 192.168.99 and data root /tmp/ts-smoke.
# Override via SMOKE_ALIAS_PREFIX / SMOKE_DATA_ROOT or CLI args.
#
# Exit codes:
#   0  all checks passed
#   1  one or more checks failed
#   2  alias IPs are not configured (see instructions printed to stderr)

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"

exec "$PYTHON_BIN" "$REPO_ROOT/scripts/smoke_test.py" "$@"