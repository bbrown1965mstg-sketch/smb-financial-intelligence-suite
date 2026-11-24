#!/bin/zsh
# Start the local PostgreSQL 17.5 server used by the SMB warehouse.
# Data directory: /Users/Owner/pgdata/smb_warehouse  (binaries: /Users/Owner/pg)
PGDATA=/Users/Owner/pgdata/smb_warehouse
PGLOG=/Users/Owner/pgdata/smb_warehouse.log
/Users/Owner/pg/bin/pg_ctl -D "$PGDATA" -l "$PGLOG" -o "-p 5432" start
echo "Postgres starting on localhost:5432 (log: $PGLOG)"
