#!/bin/zsh
# stop_suite.sh -- stop the whole GLM-5.3 SMB finance suite.
#
#   1. Kill the Streamlit servers (ports 8602 + 8603): SIGTERM, wait up to
#      10s, SIGKILL any leftovers. PIDs come from .suite_run/*.pid, with an
#      lsof port scan as the fallback (works even without a state file).
#   2. Verify both ports are freed.
#   3. Stop PostgreSQL via Data Warehouse/stop_db.sh.
#
# Prints exactly what stopped. Exits 0 cleanly when nothing is running.

set -u

SUITE_ROOT="$(cd "$(dirname "$0")" && pwd)"
RUN_DIR="$SUITE_ROOT/.suite_run"
PID_8602="$RUN_DIR/8602.pid"
PID_8603="$RUN_DIR/8603.pid"
PG_BIN="/Users/Owner/pg/bin"
PG_ISREADY="$PG_BIN/pg_isready"
DW_DIR="$SUITE_ROOT/Data Warehouse"

log()  { echo "[stop_suite] $*"; }

port_listening() { lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }
port_pids()      { lsof -nP -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null; }

is_streamlit_pid() {  # guard against PID reuse: the PID must be a streamlit process
    [[ -n "$1" ]] || return 1
    ps -p "$1" -o command= 2>/dev/null | grep -qi "streamlit"
}

# ---------------------------------------------------------------------------
# 1) collect candidate PIDs (recorded + port-based), kill them
# ---------------------------------------------------------------------------
collect_pids() {  # $1 port, $2 pidfile -> fills global array STOP_PIDS
    local port="$1" pidfile="$2" pid
    [[ -f "$pidfile" ]] && print -- "$(cat "$pidfile" 2>/dev/null)"
    port_pids "$port"
}

typeset -aU STOP_PIDS
for spec in "8602:$PID_8602" "8603:$PID_8603"; do
    port="${spec%%:*}"
    pidfile="${spec##*:}"
    for pid in $(collect_pids "$port" "$pidfile"); do
        if is_streamlit_pid "$pid"; then
            STOP_PIDS+=("$pid")
        fi
    done
done

if (( ${#STOP_PIDS} == 0 )); then
    log "No running Streamlit servers found (ports 8602 / 8603 clear)."
else
    for pid in "${STOP_PIDS[@]}"; do
        if kill -TERM "$pid" 2>/dev/null; then
            log "SIGTERM sent to Streamlit PID $pid"
        fi
    done

    # wait up to 10s for graceful shutdown
    deadline=$(( SECONDS + 10 ))
    while (( SECONDS < deadline )); do
        alive=0
        for pid in "${STOP_PIDS[@]}"; do
            ps -p "$pid" >/dev/null 2>&1 && alive=1
        done
        (( alive == 0 )) && break
        sleep 1
    done

    # SIGKILL leftovers
    for pid in "${STOP_PIDS[@]}"; do
        if ps -p "$pid" >/dev/null 2>&1; then
            if is_streamlit_pid "$pid"; then
                kill -KILL "$pid" 2>/dev/null && log "SIGKILL sent to leftover PID $pid"
            fi
        fi
    done
fi

# ---------------------------------------------------------------------------
# 2) verify the ports are freed
# ---------------------------------------------------------------------------
ports_ok=1
for port in 8602 8603; do
    if port_listening "$port"; then
        ports_ok=0
        log "WARNING: port $port is STILL listening: PID(s) $(port_pids "$port" | tr '\n' ' ')"
    else
        log "Port $port freed."
    fi
done

rm -f "$PID_8602" "$PID_8603"

# ---------------------------------------------------------------------------
# 3) PostgreSQL
# ---------------------------------------------------------------------------
if "$PG_ISREADY" -h localhost -p 5432 -t 2 >/dev/null 2>&1; then
    log "Stopping PostgreSQL via Data Warehouse/stop_db.sh ..."
    # stop_db.sh ships without the execute bit -- run it through zsh
    # explicitly rather than modifying the read-only file.
    if ( cd "$DW_DIR" && zsh stop_db.sh ); then
        log "PostgreSQL stopped (localhost:5432)."
    else
        log "WARNING: stop_db.sh returned an error -- check /Users/Owner/pgdata/smb_warehouse.log"
        ports_ok=0
    fi
else
    log "PostgreSQL was not running -- nothing to stop."
fi

# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------
if (( ports_ok == 0 )); then
    log "Suite stop finished WITH WARNINGS (see above)."
    exit 1
fi
log "Suite stopped: Doc-to-Warehouse (8603), Data Warehouse (8602), PostgreSQL (5432)."
log "Start it again with: ./launch_suite.sh"
exit 0
