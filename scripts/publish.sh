#!/usr/bin/env bash
#
# publish.sh — Publish all KyuGraph crates to crates.io in dependency order.
#
# Uses cargo login credentials (~/.cargo/credentials.toml) or .env.cargo.
# Usage:
#   ./scripts/publish.sh            # publish all crates
#   ./scripts/publish.sh --dry-run  # validate without publishing
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env.cargo"

# ---------------------------------------------------------------------------
# Load token: .env.cargo > CARGO_REGISTRY_TOKEN env > ~/.cargo/credentials.toml
# ---------------------------------------------------------------------------
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  export CARGO_REGISTRY_TOKEN
  echo "Using token from $ENV_FILE"
elif [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "Using token from environment"
else
  echo "Using token from cargo login (~/.cargo/credentials.toml)"
fi

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
# All crates in strict dependency order (flattened from tiers)
# ---------------------------------------------------------------------------
CRATES=(
  # Tier 1: no internal deps
  kyu-common
  # Tier 2: depends on kyu-common
  kyu-types
  kyu-coord
  # Tier 3: depends on Tier 1-2
  kyu-parser
  kyu-storage
  kyu-delta
  kyu-index
  kyu-extension
  kyu-copy
  # Tier 4: depends on Tier 1-3
  kyu-expression
  kyu-transaction
  kyu-catalog
  # Tier 5: depends on Tier 1-4
  kyu-binder
  ext-algo
  ext-rdf
  ext-vector
  ext-fts
  # Tier 6-8
  kyu-planner
  kyu-executor
  kyu-api
  # Tier 9: top-level
  kyu-graph
  kyu-graph-cli
  kyu-visualizer
)

# Rate limit: crates.io allows ~5 new crates per 10-minute window.
# Publish BATCH_SIZE crates, then pause BATCH_DELAY seconds.
BATCH_SIZE="${BATCH_SIZE:-4}"
BATCH_DELAY="${BATCH_DELAY:-630}"

# ---------------------------------------------------------------------------
# Auto-detect already-published crates
# ---------------------------------------------------------------------------
is_published() {
  local crate="$1"
  local log="/tmp/kyu-publish-$crate.log"
  if [[ -f "$log" ]] && grep -q "Published\|already been uploaded" "$log" 2>/dev/null; then
    return 0
  fi
  return 1
}

# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------
TOTAL=${#CRATES[@]}
PUBLISHED=0
SKIPPED=0
FAILED=0
BATCH_COUNT=0

echo ""
echo "Publishing ${TOTAL} crates (batch size: ${BATCH_SIZE}, delay: ${BATCH_DELAY}s)"
echo ""

for crate in "${CRATES[@]}"; do
  # Skip already-published crates
  if is_published "$crate"; then
    echo "  ✓  $crate (already published)"
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  echo -n "  Publishing $crate... "

  if cargo publish -p "$crate" $DRY_RUN 2>&1 | tee /tmp/kyu-publish-$crate.log | tail -1; then
    echo "  ✓  $crate"
    PUBLISHED=$((PUBLISHED + 1))
    BATCH_COUNT=$((BATCH_COUNT + 1))

    # Rate limit: pause after every BATCH_SIZE publishes
    if [[ $((BATCH_COUNT % BATCH_SIZE)) -eq 0 ]]; then
      echo ""
      echo "  ⏳ Rate limit pause: waiting ${BATCH_DELAY}s (${BATCH_COUNT} crates published)..."
      sleep "$BATCH_DELAY"
      echo ""
    fi
  else
    # Check if it's a rate limit error — retry after delay
    if grep -q "429 Too Many Requests" /tmp/kyu-publish-$crate.log 2>/dev/null; then
      echo "  ⏳  $crate (rate limited, waiting ${BATCH_DELAY}s then retrying...)"
      sleep "$BATCH_DELAY"

      echo -n "  Retrying $crate... "
      if cargo publish -p "$crate" $DRY_RUN 2>&1 | tee /tmp/kyu-publish-$crate.log | tail -1; then
        echo "  ✓  $crate"
        PUBLISHED=$((PUBLISHED + 1))
        BATCH_COUNT=$((BATCH_COUNT + 1))

        if [[ $((BATCH_COUNT % BATCH_SIZE)) -eq 0 ]]; then
          echo ""
          echo "  ⏳ Rate limit pause: waiting ${BATCH_DELAY}s (${BATCH_COUNT} crates published)..."
          sleep "$BATCH_DELAY"
          echo ""
        fi
      else
        echo "  ✗  $crate FAILED (after retry)"
        echo "     See /tmp/kyu-publish-$crate.log for details"
        FAILED=$((FAILED + 1))
        echo ""
        echo "Stopping: $crate failed after retry. Fix and re-run."
        exit 1
      fi
    else
      echo "  ✗  $crate FAILED"
      echo "     See /tmp/kyu-publish-$crate.log for details"
      FAILED=$((FAILED + 1))
      echo ""
      echo "Stopping: $crate failed. Fix the issue and re-run."
      exit 1
    fi
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
  echo "Install:"
  echo "  cargo install kyu-graph       # library (for use as dependency)"
  echo "  cargo install kyu-graph-cli   # interactive Cypher shell"
  echo "  cargo install kyu-visualizer  # interactive graph visualizer"
fi
