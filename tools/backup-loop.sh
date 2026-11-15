#!/usr/bin/env sh
# Entry point of the compose "backup" service: one backup every BACKUP_INTERVAL_HOURS.
# A failed backup is logged loudly and retried at the next interval; it never stops the loop.
INTERVAL_HOURS="${BACKUP_INTERVAL_HOURS:-24}"
while true; do
  sh /tools/backup.sh /backups || echo "BACKUP FAILED at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sleep "$((INTERVAL_HOURS * 3600))"
done
