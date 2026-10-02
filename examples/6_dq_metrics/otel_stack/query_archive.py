"""Query the long-term S3 archive with SQL (DuckDB), without Prometheus, Tempo or Loki.

The collector writes every OTLP batch it receives to s3://vowl-otel-archive/otlp/
as JSON, kept with no expiry. This script reads the archived traces and builds
two views:

- runs: one row per validation run (contract, domain, check counts, pass rate)
- checks: one row per check per run (status, row counts, dimension)

Run from this folder (or `make otel-query-archive`):

    uv run python query_archive.py
    uv run python query_archive.py --sql "SELECT * FROM checks WHERE status = 'FAILED'"
"""

import argparse

import duckdb

SETUP = """
INSTALL httpfs; LOAD httpfs;
CREATE SECRET archive (TYPE s3, KEY_ID 'vowl', SECRET 'vowl-local', REGION 'us-east-1',
                       ENDPOINT '$ENDPOINT', URL_STYLE 'path', USE_SSL false);

-- An OTLP AnyValue holds one of stringValue, intValue, doubleValue or boolValue. Return it as text.
CREATE MACRO kv(v) AS coalesce(v->>'stringValue', v->>'intValue', v->>'doubleValue', v->>'boolValue');

-- One row per span, with its resource attributes (contract, domain, service) beside it.
CREATE VIEW spans AS
WITH rs AS (
    SELECT unnest(resourceSpans) AS rs
    FROM read_json('s3://vowl-otel-archive/otlp/**/traces_*.json', maximum_object_size = 104857600,
                   columns = {'resourceSpans': 'JSON[]'})
), ss AS (
    SELECT rs->'resource'->'attributes' AS res, unnest((rs->'scopeSpans')::JSON[]) AS ss FROM rs
), s AS (
    SELECT res, unnest((ss->'spans')::JSON[]) AS span FROM ss
)
SELECT
    to_timestamp((span->>'startTimeUnixNano')::HUGEINT / 1e9) AS started_at,
    span->>'name' AS name,
    span->>'traceId' AS trace_id,
    map_from_entries([{'k': a->>'key', 'v': kv(a->'value')}
                      for a in res::JSON[]]) AS res,
    map_from_entries([{'k': a->>'key', 'v': kv(a->'value')}
                      for a in (span->'attributes')::JSON[]]) AS attr
FROM s;

CREATE VIEW runs AS
SELECT started_at, res['vowl.run.id'] AS run_id, res['service.name'] AS service,
       res['vowl.domain'] AS domain, res['vowl.contract.name'] AS contract,
       res['vowl.contract.version'] AS contract_version,
       attr['check.count.passed']::INT AS checks_passed,
       attr['check.count.failed']::INT AS checks_failed,
       round(attr['check.pass_rate']::DOUBLE, 4) AS check_pass_rate,
       trace_id
FROM spans WHERE name = 'vowl.validate';

CREATE VIEW checks AS
SELECT started_at, res['vowl.run.id'] AS run_id, res['vowl.domain'] AS domain,
       res['vowl.contract.name'] AS contract, attr['schema_name'] AS schema_name,
       attr['check_name'] AS check_name, attr['dimension'] AS dimension, attr['status'] AS status,
       attr['row.count.failed']::BIGINT AS rows_failed, attr['row.count.passed']::BIGINT AS rows_passed,
       trace_id
FROM spans WHERE name = 'vowl.check';
"""

DEFAULT_QUERIES = {
    "Runs per contract": """
        SELECT domain, contract, count(*) AS runs, min(started_at) AS first_run, max(started_at) AS last_run,
               round(avg(check_pass_rate), 4) AS avg_check_pass_rate
        FROM runs GROUP BY ALL ORDER BY domain, contract""",
    "Checks that failed most often": """
        SELECT contract, schema_name, check_name, dimension,
               count(*) FILTER (WHERE status = 'FAILED') AS times_failed, count(*) AS times_run
        FROM checks GROUP BY ALL HAVING times_failed > 0 ORDER BY times_failed DESC, contract LIMIT 10""",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="localhost:8333", help="S3 endpoint, host:port")
    parser.add_argument("--sql", help="your own query against the runs, checks or spans views")
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute(SETUP.replace("$ENDPOINT", args.endpoint))
    queries = {"Your query": args.sql} if args.sql else DEFAULT_QUERIES
    for title, sql in queries.items():
        print(f"\n{title}")
        con.sql(sql).show(max_width=200)


if __name__ == "__main__":
    main()
