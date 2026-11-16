#!/usr/bin/env sh
# Entry point of the compose "backup" service: one backup every BACKUP_INTERVAL_HOURS.
# A failed backup is logged loudly and retried at the next interval; it never stops the loop.
# With ALERT_WEBHOOK_URL set (Slack, Teams or similar), a failure is also posted there.
INTERVAL_HOURS="${BACKUP_INTERVAL_HOURS:-24}"
while true; do
  if ! sh /tools/backup.sh /backups; then
    MSG="BACKUP FAILED at $(date -u +%Y-%m-%dT%H:%M:%SZ) for database ${PGDATABASE:-?}"
    echo "$MSG"
    if [ -n "${ALERT_WEBHOOK_URL:-}" ]; then
      wget -q -T 10 -O /dev/null --header "Content-Type: application/json" \
        --post-data "{\"text\": \"Foresentry: $MSG\"}" "$ALERT_WEBHOOK_URL" || echo "alert webhook failed"
    fi
  fi
  sleep "$((INTERVAL_HOURS * 3600))"
done
