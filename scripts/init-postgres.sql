-- Separate databases so the local stack mirrors the cluster's separation of
-- concerns: Nessie's version store, Airflow's metadata, and the serving layer
-- are three different systems in OpenShift.
CREATE DATABASE nessie;
CREATE DATABASE airflow;
CREATE DATABASE serving;
-- Live-content-set bookkeeping for the Nessie GC tool. Separate again: it is
-- GC's own working state, not catalog data, and it is safe to drop and rebuild.
CREATE DATABASE nessie_gc;

-- Marquez, the OpenLineage consumer (compose profile `lineage`). Its own role
-- and database rather than `platform`'s, because the image's marquez.dev.yml
-- HARDCODES db name, user and password to `marquez` -- only host and port are
-- read from the environment. Created here so a fresh stack needs no manual
-- step; an EXISTING stack has already run this file and needs the same two
-- statements applied by hand, because docker-entrypoint-initdb.d runs once,
-- on an empty data directory.
CREATE ROLE marquez WITH LOGIN PASSWORD 'marquez';
CREATE DATABASE marquez OWNER marquez;
