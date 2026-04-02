---
hide:
  - navigation
---

<div align="center">
  <img src="img/vowl_logo.png" alt="vowl logo" width="400">
</div>

# vowl

vowl (vee-owl 🦉): a validation engine for [Open Data Contract Standard (ODCS)](https://github.com/bitol-io/open-data-contract-standard) data contracts. Define your validation rules once in a declarative YAML contract and get rich, actionable reports on your data's quality.

## Key Features

- **Extensible Check Engine:** Ships with a SQL check engine out of the box, with the architecture designed to support custom check types beyond SQL.
- **Auto-Generated Rules:** Checks are automatically derived from contract metadata (`logicalType`, `logicalTypeOptions`, `required`, `unique`, `primaryKey`) and library metrics (`nullValues`, `missingValues`, `invalidValues`, `duplicateValues`, `rowCount`).
- **Any DataFrame, Any Backend:** Load any [Narwhals-compatible](https://github.com/narwhals-dev/narwhals) DataFrame type (pandas, Polars, PySpark, etc.) or connect to **20+ backends** via [Ibis](https://github.com/ibis-project/ibis). SQL dialect translation is handled by [SQLGlot](https://github.com/tobymao/sqlglot).
- **Server-Side Execution:** SQL checks run server-side through Ibis without materialising tables on the client.
- **Multi-Source Validation:** Validate across tables in different source systems with cross-database joins.
- **Declarative ODCS Contracts:** Define validation rules in YAML following the [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard).
- **Flexible Filtering:** Filter conditions with wildcard pattern matching, ideal for incremental validation of new data.
- **Rich Reporting:** Detailed summaries, row-level failure analysis, saveable reports, and a chainable `ValidationResult` API.
- **No Silent Gaps:** Unimplemented or unrecognised checks surface as `ERROR`, not quietly skipped, so nothing slips through the cracks.

## Quick Start

```bash
pip install vowl
```

```python
import pandas as pd
from vowl import validate_data

df = pd.read_csv("data.csv")
result = validate_data("contract.yaml", df=df)
result.display_full_report()
```

Optional extras: `vowl[spark]`, `vowl[all]`.

## License

This project is licensed under the [MIT License](https://github.com/govtech-data-practice/vowl/blob/main/LICENSE).
