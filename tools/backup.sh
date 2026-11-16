#!/usr/bin/env sh
# Back up the Foresentry PostgreSQL database to a compressed, checksummed archive.
#   tools/backup.sh [output-dir]              one backup now (default dir: backups)
# Reads PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE.
# Archives older than BACKUP_KEEP_DAYS (default 14) are removed afterwards.
# The file is a pg_dump custom-format archive: restore it with tools/restore.sh.
set -eu
OUT_DIR="${1:-backups}"
KEEP_DAYS="${BACKUP_KEEP_DAYS:-14}"
: "${PGDATABASE:?set PGDATABASE}"
mkdir -p "$OUT_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
FILE="$OUT_DIR/${PGDATABASE}_${STAMP}.dump"
# Write under a temporary name first so a failed dump never looks like a good backup.
pg_dump --format=custom --compress=6 --no-owner --file "$FILE.partial"
pg_restore --list "$FILE.partial" > /dev/null      # the archive must be readable
mv "$FILE.partial" "$FILE"
(cd "$OUT_DIR" && sha256sum "$(basename "$FILE")" > "$(basename "$FILE").sha256")
echo "backup written: $FILE ($(wc -c < "$FILE") bytes)"
# Retention: only this database's archives, only directly inside the backup directory.
find "$OUT_DIR" -maxdepth 1 -type f \( -name "${PGDATABASE}_*.dump" -o -name "${PGDATABASE}_*.dump.sha256" \) \
  -mtime "+$KEEP_DAYS" -print -delete | sed 's/^/removed old backup: /'
