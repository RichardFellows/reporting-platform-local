-- Separate databases so the local stack mirrors the cluster's separation of
-- concerns: Nessie's version store, Airflow's metadata, and the serving layer
-- are three different systems in OpenShift.
CREATE DATABASE nessie;
CREATE DATABASE airflow;
CREATE DATABASE serving;
-- Live-content-set bookkeeping for the Nessie GC tool. Separate again: it is
-- GC's own working state, not catalog data, and it is safe to drop and rebuild.
CREATE DATABASE nessie_gc;
