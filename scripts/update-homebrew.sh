#!/usr/bin/env bash
#
# update-homebrew.sh — Update the Homebrew formula after a GitHub release.
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
FORMULA_PATH="$TAP_REPO_PATH/Formula/kyu-graph-cli.rb"

if [[ ! -f "$FORMULA_PATH" ]]; then
  echo "Error: Formula not found at $FORMULA_PATH"
  echo "Set TAP_REPO_PATH to the homebrew-kyugraph repo root."
  exit 1
fi

echo "Fetching checksums for release $TAG..."

TARGETS=(
  aarch64-apple-darwin
  x86_64-apple-darwin
  x86_64-unknown-linux-gnu
  aarch64-unknown-linux-gnu
)

declare -A SHAS

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

for target in "${TARGETS[@]}"; do
  ARCHIVE="kyu-graph-cli-${VERSION}-${target}.tar.gz"
  SHA_FILE="${ARCHIVE}.sha256"

  echo "  Downloading checksum for $target..."
  gh release download "$TAG" \
    --repo offbit-ai/kyugraph \
    --pattern "$SHA_FILE" \
    --dir "$TMPDIR"

  SHA="$(awk '{print $1}' "$TMPDIR/$SHA_FILE")"
  SHAS[$target]="$SHA"
  echo "    $target: $SHA"
done

echo ""
echo "Updating formula at $FORMULA_PATH..."

# Update version (perl is portable across macOS and Linux)
perl -i -pe "s/^  version \".*\"/  version \"${VERSION}\"/" "$FORMULA_PATH"

# Update SHA256 values per target (match url line → next sha256 line)
python3 - "$FORMULA_PATH" "$VERSION" \
  "${SHAS[aarch64-apple-darwin]}" \
  "${SHAS[x86_64-apple-darwin]}" \
  "${SHAS[x86_64-unknown-linux-gnu]}" \
  "${SHAS[aarch64-unknown-linux-gnu]}" <<'PYEOF'
import sys, re

formula_path = sys.argv[1]
version = sys.argv[2]
sha_macos_arm64 = sys.argv[3]
sha_macos_x86 = sys.argv[4]
sha_linux_x86 = sys.argv[5]
sha_linux_arm64 = sys.argv[6]

with open(formula_path, "r") as f:
    content = f.read()

replacements = {
    "aarch64-apple-darwin": sha_macos_arm64,
    "x86_64-apple-darwin": sha_macos_x86,
    "x86_64-unknown-linux-gnu": sha_linux_x86,
    "aarch64-unknown-linux-gnu": sha_linux_arm64,
}

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

print(f"Formula updated to version {version}")
PYEOF

echo ""
echo "Formula updated successfully."

if [[ "$PUSH" == true ]]; then
  echo ""
  echo "Committing and pushing to tap repo..."
  cd "$TAP_REPO_PATH"
  git add Formula/kyu-graph-cli.rb
  git commit -m "kyu-graph-cli ${VERSION}"
  git push origin main
  echo "Pushed to offbit-ai/homebrew-kyugraph."
else
  echo ""
  echo "To commit and push, re-run with --push:"
  echo "  $0 $TAG --push"
fi
