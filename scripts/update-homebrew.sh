#!/usr/bin/env bash
#
# update-homebrew.sh — Update Homebrew formulae after a GitHub release.
#
# Updates both kyu-graph-cli and kyu-viz formulae.
#
# Usage:
#   ./scripts/update-homebrew.sh v0.1.0
#   ./scripts/update-homebrew.sh v0.1.0 --push
#
# Prerequisites:
#   - gh CLI authenticated with access to offbit-ai/kyugraph
#   - The offbit-ai/homebrew-kyugraph repo cloned as a sibling directory,
#     or set TAP_REPO_PATH to its location.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <version-tag> [--push]"
  echo "  e.g. $0 v0.1.0"
  exit 1
fi

TAG="$1"
VERSION="${TAG#v}"
PUSH=false
if [[ "${2:-}" == "--push" ]]; then
  PUSH=true
fi

TAP_REPO_PATH="${TAP_REPO_PATH:-$REPO_ROOT/../homebrew-kyugraph}"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

fetch_sha() {
  local binary="$1"
  local target="$2"
  local archive="${binary}-${VERSION}-${target}.tar.gz"
  local sha_file="${archive}.sha256"

  echo "  Downloading checksum for ${binary}/${target}..." >&2
  gh release download "$TAG" \
    --repo offbit-ai/kyugraph \
    --pattern "$sha_file" \
    --dir "$TMPDIR"

  awk '{print $1}' "$TMPDIR/$sha_file"
}

update_formula() {
  local binary="$1"
  local formula_path="$TAP_REPO_PATH/Formula/${binary}.rb"

  if [[ ! -f "$formula_path" ]]; then
    echo "  Skipping $binary — formula not found at $formula_path"
    return
  fi

  echo ""
  echo "=== Updating $binary ==="

  # Fetch checksums for all targets this binary ships.
  local targets=()
  if [[ "$binary" == "kyu-graph-cli" ]]; then
    targets=(aarch64-apple-darwin x86_64-apple-darwin x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu)
  elif [[ "$binary" == "kyu-viz" ]]; then
    # kyu-viz is macOS-only (GUI app)
    targets=(aarch64-apple-darwin x86_64-apple-darwin)
  fi

  # Build associative-array-like variables (bash 3 compat)
  local sha_values=""
  for target in "${targets[@]}"; do
    local sha
    sha="$(fetch_sha "$binary" "$target")"
    echo "  ${target}: ${sha}"
    sha_values="${sha_values}${target}=${sha};"
  done

  # Update version
  perl -i -pe "s/^  version \".*\"/  version \"${VERSION}\"/" "$formula_path"

  # Update SHA256 values per target
  python3 - "$formula_path" "$sha_values" <<'PYEOF'
import sys, re

formula_path = sys.argv[1]
sha_pairs = sys.argv[2]

replacements = {}
for pair in sha_pairs.strip(";").split(";"):
    if "=" in pair:
        target, sha = pair.split("=", 1)
        replacements[target] = sha

with open(formula_path, "r") as f:
    content = f.read()

lines = content.split("\n")
result = []
pending_target = None

for line in lines:
    for target in replacements:
        if target in line and "url " in line:
            pending_target = target
            break

    if pending_target and "sha256 " in line and "url " not in line:
        old_sha = re.search(r'sha256 "([^"]*)"', line)
        if old_sha:
            line = line.replace(old_sha.group(1), replacements[pending_target])
        pending_target = None

    result.append(line)

with open(formula_path, "w") as f:
    f.write("\n".join(result))
PYEOF

  echo "  Formula updated: $formula_path"
}

echo "Fetching checksums for release $TAG..."

# Update both formulae
update_formula "kyu-graph-cli"
update_formula "kyu-viz"

echo ""
echo "All formulae updated."

if [[ "$PUSH" == true ]]; then
  echo ""
  echo "Committing and pushing to tap repo..."
  cd "$TAP_REPO_PATH"
  git add Formula/
  git commit -m "Update to ${VERSION}"
  git push origin main
  echo "Pushed to offbit-ai/homebrew-kyugraph."
else
  echo ""
  echo "To commit and push, re-run with --push:"
  echo "  $0 $TAG --push"
fi
