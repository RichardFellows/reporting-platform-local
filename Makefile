# Automated route. The manual walkthrough in README.md does the same thing
# step by step — use that first if you want to see what each stage does.

SHELL := /bin/bash
COMPOSE := docker compose
DBT := docker compose exec -T airflow dbt
DBT_ARGS := --project-dir /opt/platform/dbt --profiles-dir /opt/platform/dbt

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.PHONY: env
env: ## Create .env from the template
	@test -f .env || cp .env.example .env

.PHONY: up
up: env ## Start the whole local stack
	$(COMPOSE) up -d --build
	@echo "MinIO    http://localhost:19001 (minioadmin / minioadmin123)"
	@echo "Nessie   http://localhost:19120/api/v2/config"
	@echo "Airflow  http://localhost:8081   (admin / admin)"
	@echo "Spark    http://localhost:8080"

.PHONY: test
test: ## Config-level tests (no stack needed, ~1s). See tests/README.md
	python -m tests.run

.PHONY: down
down: ## Stop the stack, keep volumes
	$(COMPOSE) down

.PHONY: nuke
nuke: ## Stop and destroy all data
	$(COMPOSE) down -v

.PHONY: seed
seed: ## Generate sample upstream CSVs into seed/
	# --end is pinned, not defaulted to today: the README walkthrough names
	# specific generated filenames, and they all derive from this date.
	# Runs in the container, not on the host: seed/ is a bind mount so the
	# files land on your disk either way, and QUICKSTART promises you do not
	# need Python installed. --clean omits the two deliberate data-quality
	# failures, so the build that follows can actually publish.
	$(COMPOSE) exec -T airflow python /opt/platform/scripts/generate_feeds.py --months 30 --end 2026-08-19 --out /opt/platform/seed --clean

.PHONY: land
land: ## Upload seed CSVs into the S3 landing prefix
	$(COMPOSE) exec -T airflow python -m scripts.land_feeds --source /opt/platform/seed

# `pools` and `deps` both run automatically in `airflow-init` now. They are
# kept because re-running them by hand is the first thing you do when a task is
# stuck `queued` or dbt cannot find dbt_utils, and neither is destructive.
.PHONY: pools
pools: ## Re-create the write pool (airflow-init already did this)
	# ONE pool, deliberately. Ingest, dbt builds and maintenance all take this
	# same slot -- a second one-slot pool would NOT exclude them from each
	# other, which is exactly the bug that let remove_orphan_files run
	# alongside a write. See the platform_housekeeping.py docstring.
	$(COMPOSE) exec -T airflow airflow pools set lakehouse_write 1 "serialise all Iceberg writers, incl. maintenance"

.PHONY: deps
deps: ## Re-install dbt packages (airflow-init already did this)
	$(DBT) deps $(DBT_ARGS)

# NOTE: these three build on `main`, because nessie_ref defaults to main and
# nothing here overrides it. That is convenient for a throwaway local stack but
# it is NOT the write-audit-publish pattern the platform is built around: a
# failed build leaves its partial output on main rather than on an abandoned
# branch. The real orchestration (airflow/dags/dbt_builds.py) always opens a
# branch and merges only when the test task passes, and README section 8 shows
# the manual equivalent via scripts/_open_build_branch.py. Prefer that when the
# state of main matters.
.PHONY: build
build: ## Full dbt build (run + test) -- on main, see note above
	$(DBT) build $(DBT_ARGS)

.PHONY: prepared
prepared: ## Build the prepared layer only -- on main, see note above
	$(DBT) build $(DBT_ARGS) --select path:models/prepared

.PHONY: reporting
reporting: ## Build the reporting layer only -- on main, see note above
	$(DBT) build $(DBT_ARGS) --select path:models/reporting

.PHONY: lineage
lineage: ## Generate and serve the dbt lineage docs
	$(DBT) docs generate $(DBT_ARGS)
	@echo "run: docker compose exec airflow dbt docs serve --port 8083"
	@echo "(NOT 8082 -- that is the feed console)"

# --all-managed, not a hand-written --table list. These targets used to name
# five tables against the DAG's nine, so `make retention` silently left four
# growing. Both now derive from context.managed_tables().
.PHONY: retention-dry
retention-dry: ## Show what retention WOULD expire, changing nothing
	$(COMPOSE) exec -T airflow python -m reporting_platform.retention.retention \
	  --all-managed --dry-run

.PHONY: retention
retention: ## Enforce retention for real
	$(COMPOSE) exec -T airflow python -m reporting_platform.retention.retention \
	  --all-managed

.PHONY: maintenance-metrics
maintenance-metrics: ## Collect Iceberg health metrics without acting
	$(COMPOSE) exec -T airflow python -m reporting_platform.maintenance.maintain \
	  --all-managed --dry-run

.PHONY: maintenance
maintenance: ## Run metric-driven maintenance
	$(COMPOSE) exec -T airflow python -m reporting_platform.maintenance.maintain \
	  --all-managed

# `.PHONY: refs` used to sit here, immediately above `console:` -- so `console`
# was never declared phony and `refs` was declared before the recipe it names.
# A file called `console` in the repo root would have made `make console` a
# silent no-op.
.PHONY: console
console: ## Start the feed console UI on http://localhost:8082
	docker compose up -d feed-ui
	@echo "feed console: http://localhost:8082"

.PHONY: refs
refs: ## List Nessie branches and tags
	@curl -s http://localhost:19120/api/v2/trees | python3 -m json.tool
