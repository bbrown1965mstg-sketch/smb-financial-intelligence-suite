#!/bin/zsh
# Stop the local PostgreSQL 17.5 server used by the SMB warehouse.
/Users/Owner/pg/bin/pg_ctl -D /Users/Owner/pgdata/smb_warehouse stop
