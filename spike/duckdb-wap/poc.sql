-- DuckDB -> Iceberg on a Nessie branch, write-audit-publish.
--
-- Hand-runnable, phase by phase. `run.py` executes it against the running
-- stack and performs the `-- @nessie` steps, which are Nessie API calls and
-- not SQL: branching and merging are the catalog's job, not DuckDB's.
--
-- Endpoints are the compose network's, so this runs INSIDE a container:
--   docker compose exec -T airflow python - < spike/duckdb-wap/run.py
-- MinIO is only reachable as `minio:9000` from there, and that is the address
-- Nessie itself vends to clients.
--
-- READ README.md FIRST if any of this fails. The branch attach in Phase 2
-- goes through `nessie_ref_proxy.py`, and the reason is the whole finding.

-- ============================================================ Phase 0/1
-- @phase Baseline on main: can DuckDB write to Nessie at all?
INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;

-- For direct `s3://` reads and writes only. The catalog VENDS credentials for
-- its own data files -- Phase 1 needs no secret to create a table -- but a
-- bare `read_parquet('s3://...')` is not the catalog's business.
CREATE SECRET minio (
    TYPE S3, KEY_ID '${AWS_ACCESS_KEY_ID}', SECRET '${AWS_SECRET_ACCESS_KEY}',
    ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');

-- AUTHORIZATION_TYPE 'none' because this Nessie is unauthenticated; the
-- extension otherwise defaults to oauth2 and refuses to attach at all.
ATTACH 'warehouse' AS nessie_main (
    TYPE ICEBERG,
    ENDPOINT 'http://localhost:18998/iceberg',
    AUTHORIZATION_TYPE 'none');

CREATE SCHEMA IF NOT EXISTS nessie_main.wap_poc;
CREATE TABLE IF NOT EXISTS nessie_main.wap_poc.trades
    (trade_id BIGINT, book VARCHAR, notional DECIMAL(18,2));
DELETE FROM nessie_main.wap_poc.trades;          -- idempotent re-run
INSERT INTO nessie_main.wap_poc.trades VALUES (1, 'FX', 100.00);
-- @show baseline on main
SELECT count(*) AS rows FROM nessie_main.wap_poc.trades;

-- ------------------------------------------------------------ fixtures
-- One clean, one with a NULL trade_id and a duplicate, so the audit in
-- Phase 4 has something to catch and Phase 6 has a failure to prove.
COPY (SELECT * FROM (VALUES
        (20::BIGINT, 'FX', 250.00::DECIMAL(18,2)),
        (21::BIGINT, 'RATES', 400.00::DECIMAL(18,2)),
        (22::BIGINT, 'FX', 125.50::DECIMAL(18,2))
      ) t(trade_id, book, notional))
  TO 's3://lakehouse/spike/duckdb-wap/trades_good.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM (VALUES
        (10::BIGINT, 'FX', 250.00::DECIMAL(18,2)),
        (11::BIGINT, 'RATES', 400.00::DECIMAL(18,2)),
        (12::BIGINT, 'FX', 125.50::DECIMAL(18,2)),
        (11::BIGINT, 'RATES', 400.00::DECIMAL(18,2)),
        (NULL::BIGINT, 'EQ', 90.00::DECIMAL(18,2))
      ) t(trade_id, book, notional))
  TO 's3://lakehouse/spike/duckdb-wap/trades_bad.parquet' (FORMAT PARQUET);

-- ============================================================ Phase 2
-- @phase The spike: attach a branch, and prove it is isolated from main.
-- @nessie branch etl_wap_poc
ATTACH 'warehouse' AS wap (
    TYPE ICEBERG,
    -- THE REF TRAVELS IN THE PATH, and this endpoint is the PROXY's. Pointed
    -- straight at Nessie, `ATTACH` still succeeds and the branch reads as
    -- EMPTY -- see README.md, "The seam".
    ENDPOINT 'http://localhost:18998/iceberg/etl_wap_poc',
    AUTHORIZATION_TYPE 'none');
-- @show the branch sees what main saw when it was cut
SELECT count(*) AS rows FROM wap.wap_poc.trades;

-- ============================================================ Phase 3
-- @phase Write the BAD fixture on the branch.
INSERT INTO wap.wap_poc.trades
  SELECT * FROM read_parquet('s3://lakehouse/spike/duckdb-wap/trades_bad.parquet');
-- @show branch after write, and main during it
SELECT (SELECT count(*) FROM wap.wap_poc.trades) AS branch_rows,
       (SELECT count(*) FROM nessie_main.wap_poc.trades) AS main_rows;

-- ============================================================ Phase 4
-- @phase Audit. One query, one column per rule, every count zero to publish.
-- @gate
SELECT count(*) FILTER (WHERE trade_id IS NULL) AS null_ids,
       -- NOTE: `count(DISTINCT)` ignores NULL, so a NULL id also shows up
       -- here. Two rules, one row -- fine for a gate, misleading as a count.
       count(*) - count(DISTINCT trade_id)      AS dupe_ids,
       count(*) FILTER (WHERE notional <= 0)    AS bad_notional
FROM wap.wap_poc.trades;

-- ============================================================ Phase 6
-- @phase The gate failed, so discard the branch and prove main is untouched.
DETACH wap;
-- @nessie drop etl_wap_poc
-- @show main after the discard
SELECT count(*) AS rows FROM nessie_main.wap_poc.trades;

-- ============================================================ Phase 5
-- @phase Now the happy path: the good fixture, a passing gate, and a publish.
-- @nessie branch etl_wap_good
ATTACH 'warehouse' AS wap (
    TYPE ICEBERG,
    ENDPOINT 'http://localhost:18998/iceberg/etl_wap_good',
    AUTHORIZATION_TYPE 'none');
INSERT INTO wap.wap_poc.trades
  SELECT * FROM read_parquet('s3://lakehouse/spike/duckdb-wap/trades_good.parquet');
-- @gate
SELECT count(*) FILTER (WHERE trade_id IS NULL) AS null_ids,
       count(*) - count(DISTINCT trade_id)      AS dupe_ids,
       count(*) FILTER (WHERE notional <= 0)    AS bad_notional
FROM wap.wap_poc.trades;
DETACH wap;
-- @nessie merge etl_wap_good
-- @show main after the merge, on the connection that was open BEFORE it --
-- @show the stale-metadata trap, if there is one
SELECT count(*) AS rows FROM nessie_main.wap_poc.trades;
