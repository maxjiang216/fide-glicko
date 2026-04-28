#!/bin/bash
# Prepare per-function dirs for ZIP-based Lambdas (federations, tournaments, split_ids).
# Data-heavy functions (player_list, details_chunk, etc.) use the shared Docker image.
# Run before `sam build`. Uses hardlinks when possible for speed and space.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FUNCTIONS_DIR="$REPO_ROOT/.functions"

rm -rf "$FUNCTIONS_DIR"
mkdir -p "$FUNCTIONS_DIR"

# Copy handlers+src: hardlinks if supported (fast, minimal disk), else regular copy
link_or_copy() {
  local src="$1" dest="$2"
  if cp -al "$src" "$dest" 2>/dev/null; then
    : # hardlinks ok
  else
    cp -r "$src" "$dest"
  fi
}

# ZIP functions only (lightweight; data functions use Docker image)
declare -A REQS
REQS[federations]="requests>=2.32.5
beautifulsoup4>=4.14.3"
REQS[tournaments]="aiohttp>=3.10.0"
REQS[split_ids]=""
REQS[ensure_run_name]=""
REQS[orchestrator]=""

# Put scraper modules at function root so "from get_federations import" etc. resolve
for name in federations tournaments split_ids ensure_run_name orchestrator; do
  dir="$FUNCTIONS_DIR/$name"
  mkdir -p "$dir"
  link_or_copy "$REPO_ROOT/handlers" "$dir/"
  for py in "$REPO_ROOT/src/scraper"/*.py; do
    [[ -f "$py" ]] && link_or_copy "$py" "$dir/$(basename "$py")"
  done
  if [[ -n "${REQS[$name]}" ]]; then
    echo -e "${REQS[$name]}" > "$dir/requirements.txt"
  fi
  echo "Prepared $dir (zip)"
done
