# SMB Financial & Operational Data Warehouse -- Streamlit dashboard image.
#
# Base: python:3.11-slim. No apt packages needed -- the SQL schema files are
# applied through psycopg2 (docker/init_pipeline.py), never the psql CLI.
# Note: this stack was statically validated (YAML parse + logic review); it was
# not executed on the authoring machine, where Docker is not installed.

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependencies first so Docker layer caching skips reinstalls when only the
# app code changes.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Application + SQL + bootstrap assets (respects .dockerignore: uploads/,
# logs, caches, test files and .env* never enter the image; the samples/
# directory IS included -- the offline benchmark snapshot lives there. The
# two DATA-MODULE export packs enter as single CSV files only -- see
# .dockerignore -- so docker/init_pipeline.py can load the SoCal capital +
# SEC peer modules from their committed packs instead of re-harvesting; a
# missing pack degrades that module to schema-only at boot).
COPY app.py seed_demo.py fetch_benchmarks.py forecast_engine.py ingest_csv.py \
     nl_to_sql.py pricing_simulator.py ./
COPY schema.sql analytics_views.sql ingest_config.example.json ./
COPY funding_schema.sql peer_ratios_schema.sql ./
# Multi-tenant migrations (001 companies registry, 002 RLS + nl2sql_reader
# role): docker/init_pipeline.py applies migrations/*.sql on EVERY boot --
# without this COPY the AI-SQL read role and the row-level-security policies
# would never exist inside the container.
COPY migrations ./migrations
COPY samples ./samples
COPY docker ./docker
COPY docker-entrypoint.sh ./docker-entrypoint.sh
COPY exports/socal_funding_deals_v1/socal_capital_events.csv \
     exports/socal_funding_deals_v1/
COPY peer_financial_ratios_v1/peer_financials.csv peer_financial_ratios_v1/

# Non-root runtime user; /app stays read-only for it.
RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app \
    && chmod +x /app/docker-entrypoint.sh
USER appuser

EXPOSE 8602

# streamlit run app.py --server.port 8602 --server.address 0.0.0.0
ENTRYPOINT ["bash", "docker-entrypoint.sh"]
