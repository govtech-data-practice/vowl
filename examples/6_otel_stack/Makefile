# Local OpenTelemetry stack for checking vowl's export_otel end to end.
# Run from this folder (`make otel-up`) or from the repo root (`make -C examples/6_otel_stack otel-up`).
# Requires Docker. See README.md.

.PHONY: help otel-up otel-add-vowl-runs otel-logs otel-down otel-clean otel-clean-all otel-dashboard otel-query-archive

UV ?= uv
COMPOSE := docker compose -f docker-compose.yml
ARGS ?= --runs 5

help:
	@echo "Available targets:"
	@echo "  otel-up             Start the stack (collector, Prometheus, Tempo, Loki, S3 archive, Grafana)"
	@echo "  otel-add-vowl-runs  Send sample validation runs to the stack (ARGS=\"--runs 10\")"
	@echo "  otel-logs           Follow what the collector receives"
	@echo "  otel-down           Stop the stack, keeping its data"
	@echo "  otel-clean          Stop the stack and delete Prometheus, Tempo, Loki and Grafana data (keeps the S3 archive)"
	@echo "  otel-clean-all      Stop the stack and delete all data, including the S3 archive"
	@echo "  otel-dashboard      Rebuild grafana/dashboards/vowl-dq.json"
	@echo "  otel-query-archive  Query the S3 archive with SQL (SQL=\"SELECT ...\")"

otel-up:
	$(COMPOSE) up -d --wait
	@echo "Grafana dashboard: http://localhost:3000/d/vowl-dq"
	@echo "OTLP endpoints:    grpc http://localhost:4317, http http://localhost:4318"

otel-add-vowl-runs:
	$(UV) run --extra otel python send_sample_runs.py $(ARGS)
	@echo "View the runs at http://localhost:3000/d/vowl-dq"

otel-logs:
	$(COMPOSE) logs -f collector

otel-down:
	$(COMPOSE) down

otel-clean:
	$(COMPOSE) down
	docker volume rm -f vowl-otel_prometheus-data vowl-otel_tempo-data vowl-otel_loki-data vowl-otel_grafana-data

otel-clean-all:
	$(COMPOSE) down -v

otel-dashboard:
	$(UV) run python grafana/build_dashboard.py

otel-query-archive:
	$(UV) run python query_archive.py $(if $(SQL),--sql "$(SQL)")
