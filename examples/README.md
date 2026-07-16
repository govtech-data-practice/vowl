# Examples

This directory contains example code and notebooks showing how to use vowl.

Start with the **Basic Tutorial**, then reach for the topic notebooks when you need
them. Each notebook is self-contained — it re-resolves the shared dataset paths so it
runs top-to-bottom on its own.

## Notebooks

| Folder                | Notebook                 | Covers                                                                                                                        |
| --------------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `1_basic_tutorial/`   | `basic_tutorial.ipynb`   | Setup, running a validation (pandas/Polars), and understanding the results (the `ValidationResult` object & annotated output) |
| `2_multiple_sources/` | `multiple_sources.ipynb` | Validating one contract across multiple sources                                                                               |
| `3_real_databases/`   | `real_databases.ipynb`   | Server-side validation with Testcontainers (Postgres/MySQL/Spark/DuckDB ATTACH)                                               |
| `4_advanced_usage/`   | `advanced_usage.ipynb`   | Explicitly defined adapters (incl. `PooledAdapter`) and filtering rows before validation                                      |

Notebooks 1 and 2 write generated CSV/JSON artifacts into their own local `outputs/`
folder; those folders also hold pre-generated files for reference.

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
> [Testcontainers](https://testcontainers.com/)); the other notebooks run on pandas,
> Polars, and in-memory DuckDB with no external services.
