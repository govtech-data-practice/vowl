"""Write grafana/dashboards/vowl-dq.json. Re-run after editing, Grafana reloads it."""

import json
from pathlib import Path

PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}
TEMPO = {"type": "tempo", "uid": "tempo"}
# Every Prometheus query is filtered by the three dashboard variables.
F = 'service_name=~"$service", vowl_domain=~"$domain", vowl_contract_name=~"$contract"'
# Muted colours, so a dashboard full of failing checks does not glare.
BAD, WARN, GOOD, NEUTRAL = "#d16d6d", "#d9a54a", "#6baf7b", "#8ea8c3"
PCT_THRESHOLDS = {"mode": "absolute", "steps": [
    {"color": BAD, "value": None}, {"color": WARN, "value": 0.9}, {"color": GOOD, "value": 0.99}]}
RED_IF_ANY = {"mode": "absolute", "steps": [{"color": GOOD, "value": None}, {"color": BAD, "value": 1}]}
# Pass rates as a thin bar with the number beside it, counts as coloured text.
BAR_CELL = {"type": "gauge", "mode": "basic", "valueDisplayMode": "text"}
TEXT_CELL = {"type": "color-text"}

_next_id = iter(range(1, 1000))


def prom(expr, legend="", instant=False, fmt="time_series"):
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "instant": instant,
            "range": not instant, "format": fmt, "refId": "A"}


def stat(title, expr, x, unit="percentunit", w=6, thresholds=None):
    return {
        "id": next(_next_id), "type": "stat", "title": title, "datasource": PROM,
        "gridPos": {"x": x, "y": 0, "w": w, "h": 4},
        "targets": [prom(expr)],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "none"},
        "fieldConfig": {"defaults": {
            "unit": unit, "decimals": 2 if unit == "percentunit" else 0,
            "thresholds": thresholds or PCT_THRESHOLDS,
        }, "overrides": []},
    }


def timeseries(title, targets, gp, unit="percentunit", stack=False, legend="bottom"):
    return {
        "id": next(_next_id), "type": "timeseries", "title": title, "datasource": PROM,
        "gridPos": gp, "targets": targets,
        "options": {"legend": {"displayMode": "list", "placement": legend}},
        "fieldConfig": {"defaults": {"unit": unit, **({"max": 1} if unit == "percentunit" else {}), "custom": {
            "drawStyle": "line", "lineWidth": 2, "fillOpacity": 8, "gradientMode": "opacity",
            "showPoints": "auto", "pointSize": 4, "spanNulls": True,
            "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []},
    }


def latest_table(title, expr, gp, unit="percentunit", thresholds=None, desc=False):
    # A range query reduced to its last value per series, so each row is the latest run.
    return {
        "id": next(_next_id), "type": "table", "title": title, "datasource": PROM,
        "gridPos": gp, "targets": [prom(expr, legend="{{vowl_contract_name}} / {{schema_name}} / {{check_name}} ({{dimension}})")],
        "transformations": [{"id": "reduce", "options": {"reducers": ["lastNotNull"], "mode": "seriesToRows"}}],
        "fieldConfig": {"defaults": {"unit": unit, "min": 0, **({"max": 1} if unit == "percentunit" else {}),
                                     "custom": {"cellOptions": BAR_CELL if unit == "percentunit" else TEXT_CELL},
                                     "decimals": 1 if unit == "percentunit" else 0,
                                     "thresholds": thresholds or PCT_THRESHOLDS},
                        "overrides": [{"matcher": {"id": "byName", "options": "Field"},
                                       "properties": [{"id": "custom.cellOptions", "value": {"type": "auto"}},
                                                      {"id": "displayName", "value": "Check"}]}]},
        "options": {"sortBy": [{"displayName": "Last *", "desc": desc}]},
    }


def scorecard(gp):
    # One row per contract, merged from instant queries that each read the latest run.
    cols = [
        ("A", "Check pass rate", f"max by (vowl_domain, vowl_contract_name) "
                                 f"(last_over_time(vowl_run_check_pass_rate_ratio{{{F}}}[$__range]))", "percentunit"),
        ("B", "Row pass rate", f"max by (vowl_domain, vowl_contract_name) "
                               f"(last_over_time(vowl_run_row_pass_rate_ratio{{{F}}}[$__range]))", "percentunit"),
        ("C", "Checks", f"sum by (vowl_domain, vowl_contract_name) "
                        f"(last_over_time(vowl_run_check_count_total{{{F}}}[$__range]))", "short"),
        ("D", "Failed checks", f'sum by (vowl_domain, vowl_contract_name) '
                               f'(last_over_time(vowl_run_check_count_total{{{F}, status=~"FAILED|ERROR"}}[$__range]))',
         "short"),
        ("E", "Failed rows", f'max by (vowl_domain, vowl_contract_name) '
                             f'(last_over_time(vowl_run_row_count{{{F}, status="FAILED"}}[$__range]))', "short"),
    ]
    overrides = [{"matcher": {"id": "byName", "options": "vowl_contract_name"},
                  "properties": [{"id": "displayName", "value": "Contract"}]},
                 {"matcher": {"id": "byName", "options": "vowl_domain"},
                  "properties": [{"id": "displayName", "value": "Domain"}]}]
    for ref, name, _, unit in cols:
        props = [{"id": "displayName", "value": name}, {"id": "unit", "value": unit}]
        if unit == "percentunit":
            props += [{"id": "decimals", "value": 1}, {"id": "thresholds", "value": PCT_THRESHOLDS},
                      {"id": "min", "value": 0}, {"id": "max", "value": 1},
                      {"id": "custom.cellOptions", "value": BAR_CELL}]
        elif name != "Checks":
            props += [{"id": "thresholds", "value": RED_IF_ANY},
                      {"id": "custom.cellOptions", "value": TEXT_CELL}]
        overrides.append({"matcher": {"id": "byName", "options": f"Value #{ref}"}, "properties": props})
    return {
        "id": next(_next_id), "type": "table", "title": "Contract scorecard (latest run of each contract)",
        "datasource": PROM, "gridPos": gp,
        "targets": [{**prom(expr, instant=True, fmt="table"), "refId": ref} for ref, _, expr, _ in cols],
        "transformations": [
            {"id": "merge", "options": {}},
            {"id": "organize", "options": {"excludeByName": {"Time": True},
                                           "indexByName": {"vowl_domain": 0, "vowl_contract_name": 1}}},
        ],
        "fieldConfig": {"defaults": {}, "overrides": overrides},
        "options": {"sortBy": [{"displayName": "Check pass rate", "desc": False}]},
    }


panels = [
    # Catalog: every contract that matches the filters, one number or row each.
    stat("Contracts", f"count(max by (vowl_contract_name) (last_over_time(vowl_run_check_pass_rate_ratio{{{F}}}[$__range])))",
         0, unit="short", thresholds={"mode": "absolute", "steps": [{"color": NEUTRAL, "value": None}]}),
    stat("Contracts with failing checks",
         f"count(max by (vowl_contract_name) (last_over_time(vowl_run_check_pass_rate_ratio{{{F}}}[$__range])) < 1)"
         " or vector(0)", 6, unit="short", thresholds=RED_IF_ANY),
    stat("Catalog check pass rate",
         f'sum(last_over_time(vowl_run_check_count_total{{{F}, status="PASSED"}}[$__range]))'
         f" / sum(last_over_time(vowl_run_check_count_total{{{F}}}[$__range]))", 12),
    stat("Failed rows (latest runs)",
         f'sum(max by (vowl_contract_name) (last_over_time(vowl_run_row_count{{{F}, status="FAILED"}}[$__range])))',
         18, unit="short", thresholds=RED_IF_ANY),

    scorecard({"x": 0, "y": 4, "w": 24, "h": 7}),

    timeseries("Check pass rate by contract", [prom(
        f"max by (vowl_contract_name) (vowl_run_check_pass_rate_ratio{{{F}}})", "{{vowl_contract_name}}")],
        {"x": 0, "y": 11, "w": 12, "h": 8}),
    timeseries("Check pass rate by domain", [prom(
        f'sum by (vowl_domain) (vowl_run_check_count_total{{{F}, status="PASSED"}})'
        f" / sum by (vowl_domain) (vowl_run_check_count_total{{{F}}})", "{{vowl_domain}}")],
        {"x": 12, "y": 11, "w": 12, "h": 8}),

    # Drill down: schemas, dimensions and checks inside the selected contracts.
    timeseries("Schema row pass rate", [prom(
        f"max by (vowl_contract_name, schema_name) (vowl_schema_row_pass_rate_ratio{{{F}}})",
        "{{vowl_contract_name}} / {{schema_name}}")],
        {"x": 0, "y": 19, "w": 12, "h": 9}, legend="right"),
    # The worst contract per dimension, so one bad contract is not hidden by the others.
    timeseries("Dimension row pass rate (worst contract)", [prom(
        f"min by (dimension) (vowl_dimension_row_pass_rate_ratio{{{F}}})", "{{dimension}}")],
        {"x": 12, "y": 19, "w": 12, "h": 9}),

    # Check row counts are attributed rows. vowl_check_row_scalar_count holds the scalar count.
    latest_table("Check row pass rate (latest run)",
                 f"max by (vowl_contract_name, schema_name, check_name, dimension) "
                 f"(vowl_check_row_pass_rate_ratio{{{F}}})",
                 {"x": 0, "y": 28, "w": 12, "h": 10}),
    latest_table("Failed rows per check (latest run)",
                 f'max by (vowl_contract_name, schema_name, check_name, dimension) '
                 f'(vowl_check_row_count{{{F}, status="FAILED"}})',
                 {"x": 12, "y": 28, "w": 12, "h": 10}, unit="short", desc=True, thresholds=RED_IF_ANY),

    {"id": next(_next_id), "type": "table", "title": "Validation runs (Tempo, click a trace ID)",
     "datasource": TEMPO, "gridPos": {"x": 0, "y": 38, "w": 24, "h": 9},
     "targets": [{"datasource": TEMPO, "refId": "A", "queryType": "traceql", "limit": 50,
                  "query": '{ name = "vowl.validate" && resource.service.name =~ "${service:regex}"'
                           ' && resource.vowl.domain =~ "${domain:regex}"'
                           ' && resource.vowl.contract.name =~ "${contract:regex}" }'
                           ' | select(resource.vowl.contract.name, span.check.count.failed)'}]},
    {"id": next(_next_id), "type": "logs", "title": "Failed and errored checks (Loki)",
     "datasource": LOKI, "gridPos": {"x": 0, "y": 47, "w": 24, "h": 10},
     "targets": [{"datasource": LOKI, "refId": "A",
                  "expr": '{service_name=~"${service:regex}"} | vowl_domain=~"${domain:regex}"'
                          ' | vowl_contract_name=~"${contract:regex}" | line_format '
                          '"[{{.status}}] {{.vowl_contract_name}} / {{.schema_name}} / {{.check_name}}: {{__line__}}"'}],
     "options": {"showTime": True, "enableLogDetails": True, "wrapLogMessage": True}},
]


def variable(name, label, query):
    return {"name": name, "label": label, "type": "query", "datasource": PROM,
            "query": {"query": query, "refId": name}, "refresh": 2, "sort": 1,
            "includeAll": True, "multi": True, "allValue": ".+",
            "current": {"text": "All", "value": "$__all"}}


dashboard = {
    "uid": "vowl-dq", "title": "vowl DQ metrics", "schemaVersion": 39, "version": 1,
    "time": {"from": "now-1h", "to": "now"}, "refresh": "30s",
    "templating": {"list": [
        variable("service", "service.name", "label_values(vowl_run_check_pass_rate_ratio, service_name)"),
        variable("domain", "domain", 'label_values(vowl_run_check_pass_rate_ratio{service_name=~"$service"}, vowl_domain)'),
        variable("contract", "contract", "label_values(vowl_run_check_pass_rate_ratio"
                 '{service_name=~"$service", vowl_domain=~"$domain"}, vowl_contract_name)'),
    ]},
    "panels": panels,
}

out = Path(__file__).parent / "dashboards" / "vowl-dq.json"
out.write_text(json.dumps(dashboard, indent=2))
print(f"wrote {out}")
