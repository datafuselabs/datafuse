source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=databend://root:@localhost:8000/?sslmode=disable


for ((i=0; i<=3; i++)); do
    log_command bendsql --dsn \"$DSN\" -q \"OPTIMIZE TABLE t COMPACT SEGMENT LIMIT 10\"
    log_command bendsql --dsn \"$DSN\" -q \"OPTIMIZE TABLE t COMPACT LIMIT 10\"
    log_command bendsql --dsn \"$DSN\" -q \"OPTIMIZE TABLE t purge \"
done