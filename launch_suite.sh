#!/bin/zsh
# launch_suite.sh -- bring the whole GLM-5.3 SMB finance suite up.
#
#   1. PostgreSQL 17.5 (localhost:5432, PGDATA /Users/Owner/pgdata/smb_warehouse)
#   2. Doc-to-Warehouse  (Streamlit, http://localhost:8603)
#   3. Data Warehouse    (Streamlit, http://localhost:8602)
#
# Idempotent: every component that is already running is detected and NOT
# double-started; the script still exits 0 and prints all URLs.
# PIDs + logs live under .suite_run/ (see stop_suite.sh).

set -u

SUITE_ROOT="$(cd "$(dirname "$0")" && pwd)"
RUN_DIR="$SUITE_ROOT/.suite_run"
LOG_DIR="$RUN_DIR/logs"
PID_8602="$RUN_DIR/8602.pid"
PID_8603="$RUN_DIR/8603.pid"
LOG_PG="$LOG_DIR/postgres.log"
LOG_8602="$LOG_DIR/data_warehouse_8602.log"
LOG_8603="$LOG_DIR/doc_to_warehouse_8603.log"
PG_LOG_REAL="/Users/Owner/pgdata/smb_warehouse.log"

PG_BIN="/Users/Owner/pg/bin"
PG_CTL="$PG_BIN/pg_ctl"
PG_ISREADY="$PG_BIN/pg_isready"
PGDATA="/Users/Owner/pgdata/smb_warehouse"
DW_DIR="$SUITE_ROOT/Data Warehouse"
DTW_DIR="$SUITE_ROOT/Doc-to-Warehouse"
PYTHON="/usr/bin/python3"

mkdir -p "$LOG_DIR"

log()  { echo "[launch_suite] $*"; }
fail() { echo "[launch_suite] ERROR: $*" >&2; exit 1; }

port_listening() { lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }
port_pid()       { lsof -nP -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null | head -1; }

record_pid() {  # $1 port, $2 pidfile
    local pid
    pid="$(port_pid "$1")"
    if [[ -n "$pid" ]]; then
        echo "$pid" > "$2"
    fi
}

start_streamlit() {  # $1 port, $2 cwd, $3 app path (relative to cwd), $4 pidfile, $5 logfile, $6 display name
    local port="$1" cwd="$2" app="$3" pidfile="$4" logfile="$5" name="$6"
    if port_listening "$port"; then
        log "$name already listening on port $port -- not double-starting"
        record_pid "$port" "$pidfile"
        return 0
    fi
    log "Starting $name on port $port (cwd: $cwd, log: $logfile)"
    ( cd "$cwd" && nohup "$PYTHON" -m streamlit run "$app" \
        --server.port "$port" --server.headless true \
        --server.fileWatcherType none >> "$logfile" 2>&1 \
        & echo $! > "$pidfile" )
}

wait_port() {  # $1 port, $2 timeout seconds, $3 display name
    local deadline=$(( SECONDS + $2 ))
    while ! port_listening "$1"; do
        if (( SECONDS >= deadline )); then
            fail "${3:-port $1} did not start listening on port $1 within ${2}s (check its log)"
        fi
        sleep 1
    done
}

# ---------------------------------------------------------------------------
# 1) PostgreSQL 17.5
# ---------------------------------------------------------------------------
if "$PG_CTL" -D "$PGDATA" status >/dev/null 2>&1; then
    log "Postgres already running (PGDATA $PGDATA)"
else
    log "Postgres not running -- starting via Data Warehouse/start_db.sh"
    # run through zsh explicitly so a missing execute bit can never break the boot
    ( cd "$DW_DIR" && zsh start_db.sh ) >> "$LOG_PG" 2>&1
fi

if "$PG_ISREADY" -h localhost -p 5432 -t 30 >/dev/null 2>&1; then
    log "Postgres ready: localhost:5432/smb_warehouse"
else
    fail "Postgres did not become ready within 30s. Check $LOG_PG and $PG_LOG_REAL, then re-run ./launch_suite.sh"
fi

# ---------------------------------------------------------------------------
# 2) Doc-to-Warehouse  (port 8603, launched from the suite root)
# ---------------------------------------------------------------------------
start_streamlit 8603 "$SUITE_ROOT" "Doc-to-Warehouse/app.py" \
    "$PID_8603" "$LOG_8603" "Doc-to-Warehouse"

# ---------------------------------------------------------------------------
# 3) Data Warehouse  (port 8602, launched from its own directory as cwd --
#    matches the existing dashboard.log workflow there; app.py inserts its
#    own directory into sys.path, so the cwd-relative module imports load)
# ---------------------------------------------------------------------------
start_streamlit 8602 "$DW_DIR" "app.py" \
    "$PID_8602" "$LOG_8602" "Data Warehouse"

# ---------------------------------------------------------------------------
# 4) wait for both Streamlit ports
# ---------------------------------------------------------------------------
wait_port 8603 60 "Doc-to-Warehouse"
wait_port 8602 60 "Data Warehouse"

record_pid 8603 "$PID_8603"
record_pid 8602 "$PID_8602"

# ---------------------------------------------------------------------------
# 5) report
# ---------------------------------------------------------------------------
echo
echo "=============================================================="
echo " GLM-5.3 SMB finance suite is up"
echo "=============================================================="
echo "  Data Warehouse    ->  http://localhost:8602"
echo "  Doc-to-Warehouse  ->  http://localhost:8603"
echo "  PostgreSQL 17.5   ->  localhost:5432/smb_warehouse"
echo "--------------------------------------------------------------"
echo "  Postgres server log : $PG_LOG_REAL"
echo "  Postgres start log  : $LOG_PG"
echo "  Data Warehouse log  : $LOG_8602"
echo "  Doc-to-Warehouse log: $LOG_8603"
echo "  PIDs                : $PID_8602 , $PID_8603"
echo "=============================================================="
echo "  Stop everything with: ./stop_suite.sh"
echo "=============================================================="
exit 0
