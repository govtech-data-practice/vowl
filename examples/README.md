# Examples

This directory contains example code and notebooks showing how to use vowl.

Start with the **Basic Tutorial**, then reach for the topic notebooks when you need
them. Each notebook is self-contained — it re-resolves the shared dataset paths so it
runs top-to-bottom on its own.

## Notebooks

| Folder                | Notebook                 | Covers                                                                                                                        |
| --------------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `1_basic_tutorial/`   | `basic_tutorial.ipynb`   | Setup, running a validation (pandas/Polars), and the `ValidationResult` object (summary, check results, failed rows, saving)  |
| `2_multiple_sources/` | `multiple_sources.ipynb` | Validating one contract across multiple sources                                                                               |
| `3_real_databases/`   | `real_databases.ipynb`   | Server-side validation with Testcontainers (Postgres/MySQL/Spark/DuckDB ATTACH)                                               |
| `4_advanced_usage/`   | `advanced_usage.ipynb`   | Explicitly defined adapters (incl. `PooledAdapter`) and filtering rows before validation                                      |
| `5_outputs/`          | `outputs_tour.ipynb`     | Every result shape: annotated tables, residual rows, saving to disk, and OpenTelemetry export (OTEL needs the `[otel]` extra) |
| `6_otel_stack/`       | `README.md`              | A local OTel stack (Collector, Prometheus, Tempo, Loki, Grafana) and a DQ dashboard for four sample contracts. Needs Docker   |

Notebooks 1, 2, and 5 write generated artifacts into their own local `outputs/`
folder, which also holds pre-generated files for reference. `5_outputs/` is the
fullest: it keeps the annotated tables and residues as CSV, the run
`*_summary.json`, and a serialized sample of each OpenTelemetry signal
(`metrics.json`, `traces.json`, `logs.json`), so you can see every exported shape
without running anything.

## Other files

| File             | Description                                               |
| ---------------- | --------------------------------------------------------- |
| `basic_usage.py` | Minimal script: validate a CSV with pandas in a few lines |

## Running Examples

```bash
# From the project root: run the basic script
uv run python examples/basic_usage.py

# Or open a notebook in VS Code / Jupyter (start with the Basic Tutorial)
jupyter lab examples/1_basic_tutorial/basic_tutorial.ipynb
```

> **Note:** `3_real_databases/real_databases.ipynb` requires Docker (via
> [Testcontainers](https://testcontainers.com/)). The OpenTelemetry section of
> `5_outputs/outputs_tour.ipynb` needs the `[otel]` extra (`pip install 'vowl[otel]'`)
> but runs fully offline against in-memory OpenTelemetry providers. The other
> notebooks run on pandas, Polars, and in-memory DuckDB with no external services.
>
> `6_otel_stack/` also needs Docker. Start it with `make otel-up` in that folder, send sample
> runs with `make otel-add-vowl-runs`, and open <http://localhost:3000/d/vowl-dq>. See its
> [README](6_otel_stack/README.md).
