#!/usr/bin/env sh
# Restore a VYROX backup into a new database.
#   tools/restore.sh <backup.dump> <target-database>
# The target database is created here and must not already exist: this script
# never overwrites a database. To replace the live one, restore to a new name,
# check it, then point DATABASE_URL at it.
set -eu
FILE="${1:?usage: restore.sh <backup.dump> <target-database>}"
TARGET="${2:?usage: restore.sh <backup.dump> <target-database>}"
if [ -f "$FILE.sha256" ]; then
  (cd "$(dirname "$FILE")" && sha256sum -c "$(basename "$FILE").sha256") \
    || { echo "checksum mismatch: refusing to restore"; exit 1; }
else
  echo "warning: no checksum file next to $FILE"
fi
createdb "$TARGET"
pg_restore --no-owner --exit-on-error --dbname "$TARGET" "$FILE"
echo "restored $FILE into database $TARGET"
psql --dbname "$TARGET" --tuples-only --no-align --command \
  "SELECT 'predictions=' || (SELECT count(*) FROM predictions) || ' alerts=' || (SELECT count(*) FROM alerts)
       || ' alert_events=' || (SELECT count(*) FROM alert_events) || ' labels=' || (SELECT count(*) FROM labels)
       || ' users=' || (SELECT count(*) FROM users) || ' audit_log=' || (SELECT count(*) FROM audit_log)"
