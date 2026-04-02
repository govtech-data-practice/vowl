# Examples

This directory contains example code and notebooks showing how to use vowl.

## Files

| File | Description |
|------|-------------|
| `basic_usage.py` | Minimal script: validate a CSV with pandas in a few lines |
| `vowl_usage_patterns_demo.ipynb` | Interactive notebook covering pandas, Polars, Ibis/DuckDB, multi-source validation, and more |
| `demo_outputs/` | Pre-generated output files from the notebook for reference |

## Running Examples

```bash
# From the project root: run the basic script
uv run python examples/basic_usage.py

# Or open the notebook in VS Code / Jupyter
jupyter lab examples/vowl_usage_patterns_demo.ipynb
```
