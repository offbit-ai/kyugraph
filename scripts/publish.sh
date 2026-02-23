#!/usr/bin/env bash
#
# publish.sh — Publish all KyuGraph crates to crates.io in dependency order.
#
# Reads CARGO_REGISTRY_TOKEN from .env.cargo in the repo root.
# Usage:
#   ./scripts/publish.sh            # publish all crates
#   ./scripts/publish.sh --dry-run  # validate without publishing
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env.cargo"

# ---------------------------------------------------------------------------
# Load token from .env.cargo
# ---------------------------------------------------------------------------
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: $ENV_FILE not found."
  echo "Create it with: echo 'CARGO_REGISTRY_TOKEN=cio_...' > .env.cargo"
  exit 1
fi

# Source only CARGO_REGISTRY_TOKEN, ignore everything else
CARGO_REGISTRY_TOKEN="$(grep -E '^CARGO_REGISTRY_TOKEN=' "$ENV_FILE" | head -1 | cut -d'=' -f2-)"

if [[ -z "$CARGO_REGISTRY_TOKEN" ]]; then
  echo "Error: CARGO_REGISTRY_TOKEN not set in $ENV_FILE"
  exit 1
fi

export CARGO_REGISTRY_TOKEN

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN="--dry-run"
  echo "=== DRY RUN MODE ==="
  echo ""
fi

# ---------------------------------------------------------------------------
# Publishing tiers (dependency order)
# ---------------------------------------------------------------------------
# Tier 1: no internal deps
# Tier 2: depends on Tier 1
# ...
# Tier 9: depends on all prior tiers
#
# Within a tier, crates are independent and order doesn't matter.
# ---------------------------------------------------------------------------

declare -a TIER_NAMES=(
  "Tier 1 — no internal deps"
  "Tier 2 — depends on kyu-common"
  "Tier 3 — depends on Tier 1-2"
  "Tier 4 — depends on Tier 1-3"
  "Tier 5 — depends on Tier 1-4"
  "Tier 6 — depends on Tier 1-5"
  "Tier 7 — depends on Tier 1-6"
  "Tier 8 — depends on Tier 1-7"
  "Tier 9 — top-level crates"
)

declare -a TIERS=(
  "kyu-common"
  "kyu-types kyu-coord"
  "kyu-parser kyu-storage kyu-delta kyu-index kyu-extension kyu-copy"
  "kyu-expression kyu-transaction kyu-catalog"
  "kyu-binder ext-algo ext-vector ext-fts"
  "kyu-planner"
  "kyu-executor"
  "kyu-api"
  "kyu-graph kyu-graph-cli"
)

# Crates that should be skipped (publish = false or placeholders)
SKIP_CRATES="${SKIP_CRATES:-}"

# Index propagation delay between tiers (seconds)
TIER_DELAY=30

# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------
TOTAL=0
PUBLISHED=0
SKIPPED=0
FAILED=0

for i in "${!TIERS[@]}"; do
  tier="${TIERS[$i]}"
  tier_name="${TIER_NAMES[$i]}"

  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  $tier_name"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""

  for crate in $tier; do
    TOTAL=$((TOTAL + 1))

    # Check skip list
    if echo "$SKIP_CRATES" | grep -qw "$crate"; then
      echo "  ⏭  $crate (skipped)"
      SKIPPED=$((SKIPPED + 1))
      continue
    fi

    echo -n "  Publishing $crate..."

    if cargo publish -p "$crate" $DRY_RUN --token "$CARGO_REGISTRY_TOKEN" 2>&1 | tee /tmp/kyu-publish-$crate.log | tail -1; then
      echo "  ✓  $crate"
      PUBLISHED=$((PUBLISHED + 1))
    else
      echo "  ✗  $crate FAILED"
      echo "     See /tmp/kyu-publish-$crate.log for details"
      FAILED=$((FAILED + 1))
      # Stop on first failure — downstream crates will fail anyway
      echo ""
      echo "Stopping: $crate failed. Fix the issue and re-run."
      echo "Crates already published in this run: $PUBLISHED"
      exit 1
    fi
  done

  echo ""

  # Wait for crates.io index to propagate between tiers (skip after last tier and in dry-run)
  if [[ $i -lt $((${#TIERS[@]} - 1)) ]] && [[ -z "$DRY_RUN" ]]; then
    echo "  Waiting ${TIER_DELAY}s for index propagation..."
    sleep "$TIER_DELAY"
  fi
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Done"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Total:     $TOTAL"
echo "  Published: $PUBLISHED"
echo "  Skipped:   $SKIPPED"
echo "  Failed:    $FAILED"
echo ""

if [[ -n "$DRY_RUN" ]]; then
  echo "Dry run complete. Run without --dry-run to publish for real."
else
  echo "Install the CLI:"
  echo "  cargo install kyu-graph   # library (for use as dependency)"
  echo "  cargo install kyu-graph-cli     # interactive Cypher shell"
fi
