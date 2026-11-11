#!/usr/bin/env bash
# Daily check (cron): are FIDE's tournament report pages serving data again?
# One request. From 2026-11-11 they returned HTTP 200 with an empty body,
# which stopped the backfill. Notifies once when a real page comes back.
set -u
cd "$(dirname "$0")/.."
UA="fide-glicko/1.0 (chess rating research; +https://github.com/maxjiang216/fide-glicko)"
URL="https://ratings.fide.com/tournament_src_report.phtml?code=365437"
LOG=runs/fide-reports-check.log
STATE=runs/.fide-reports-back
mkdir -p runs
size=$(curl -s -o /dev/null -A "$UA" --connect-timeout 20 -m 60 -w "%{http_code} %{size_download}" "$URL")
echo "$(date '+%F %T') $size" >> "$LOG"
code=${size% *}; bytes=${size#* }
if [[ "$code" == 200 && "$bytes" -gt 5000 && ! -e "$STATE" ]]; then
  touch "$STATE"
  export DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/$(id -u)/bus"
  notify-send -u critical "FIDE reports are back" \
    "Report pages serve data again ($bytes bytes). Resume: run-exp backfill-all uv run scripts/run_month_local.py --all-prod" || true
fi
