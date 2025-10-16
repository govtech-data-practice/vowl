# Data Quality Toolkit (dqmk)

A powerful, SQL-based data quality validation library that works seamlessly with both **pandas** and **Spark** DataFrames. Define your validation rules once in a simple YAML file and get rich, actionable reports on your data's health.

`dqmk` is designed for both interactive analysis by data scientists and robust, automated checks within production data pipelines.

## 🚀 Key Features

*   **SQL-Powered Rules**: Leverage the full power of SQL for your validation logic. If you can write it in a `SELECT` statement, you can make it a data quality check.
*   **Automatic Engine Detection**: The library intelligently uses **DuckDB** for pandas and **Spark SQL** for Spark, requiring no configuration from you.
*   **Fluent Interface**: A clean, chainable `ValidationResult` object makes interacting with your results simple and intuitive.
*   **Rich, Actionable Reporting**: Go beyond simple pass/fail. Get detailed summaries, row-level failure analysis, and saveable reports out of the box.
*   **Declarative Contracts**: Define all your data quality rules in a clean, version-controllable YAML "Data Contract" following the Open Data Contract Standard (ODCS).

## 🧑‍💻 Developer Setup

Follow these instructions to set up the project for local development.

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd dqmk
```

### 2. Create and Activate a Virtual Environment

It is highly recommended to use a virtual environment to manage project dependencies.
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3.1 Install in Editable Mode

Install the package and its dependencies. The `-e` flag allows you to make changes to the source code and have them immediately reflected.
```bash
# For pandas support
pip install -e '.[pandas]'

# For spark support
pip install -e '.[spark]'

# For both pandas and spark
pip install -e '.[all]'
```

### 3.2 Install from Wheel
First, build the wheel:

```bash
# Clean previous builds
rm -rf dist/ build/ *.egg-info src/*.egg-info

# Build the package
python -m build
```

Then install from the wheel:

```bash
# For pandas support
pip install 'dist/dataquality-1.0.0-py3-none-any.whl[pandas]'

# For spark support
pip install 'dist/dataquality-1.0.0-py3-none-any.whl[spark]'

# For both
pip install 'dist/dataquality-1.0.0-py3-none-any.whl[all]'
```

### 4. Run the Demos

The repository includes demo scripts to showcase the library's functionality.

**Pandas Demo:**
```bash
python test/pandas_demo.py
```

**Spark Demo:**
```bash
python test/spark_demo.py
```
You should see a validation summary printed to your console. The scripts will also generate result files in the project's root directory.

## 🎯 The Core Concept: The Data Contract

Instead of writing validation logic in Python, you declare it in a YAML file. This separates your rules from your code, making them easier to manage, version, and share.

**Example `hdb_resale.yaml`:**
```yaml
kind: DataContract
apiVersion: v3.0.2
schema:
  - name: hdb_resale_prices # This becomes the table name in your SQL queries
    properties:
      # --- Column-Level Check ---
      - name: resale_price
        quality:
          - type: sql
            name: "resale_price_positive"
            query: "SELECT COUNT(*) FROM hdb_resale_prices WHERE resale_price <= 0"
            mustBe: 0

\      - name: flat_type
        quality:
          - type: sql
            name: "flat_type_enum"
            query: "SELECT COUNT(*) FROM hdb_resale_prices WHERE flat_type NOT IN ('3 ROOM', '4 ROOM', '5 ROOM', 'EXECUTIVE')"
            mustBe: 0

    # --- Table-Level Check ---
    quality:
      - type: sql
        name: "no_null_resale_prices"
        query: "SELECT COUNT(*) FROM hdb_resale_prices WHERE resale_price IS NULL"
        mustBe: 0
```

## ⚡ Quick Start: Validate in 3 Lines

This is the fastest way to see `dqmk` in action.

```python
import pandas as pd
from dataquality import validate_data

# 1. Load your data
df = pd.read_csv("test/HDBResale.csv")

# 2. Run the validation (context manager handles cleanup automatically)
with validate_data(df, contract_path="src/dataquality/contracts/hdb_resale.yaml") as result:
    # 3. Get a complete report
    result.display_full_report()
```

**Output:**
```
=== Data Quality Validation Results ===

 OVERALL DATA QUALITY
   Data Quality:     99.997%
   Clean Records:         201,873 of 201,879 rows
   Records with Issues:   6 row(s)

 VALIDATION CHECKS
   Total Rules Executed:  13
   Rules Passed:          9
   Rules Failed:          4
   Check Pass Rate:       69.2%

 PERFORMANCE
   Total Execution:       836.67 ms

=== Sample of Failed Rows (5 of 7 total) ===
      month        town flat_type block       street_name storey_range  floor_area_sqm  ...
0   2017-01  ANG MO KIO    3 ROOM   219  ANG MO KIO AVE 1     07 TO 09            67.0  ...
...

Results saved:
   - Data:    dq_results_enhanced_data.csv
   - Summary: dq_results_summary.json
```

---

## 🔧 The `ValidationResult` Object: Your Toolkit

The `validate_data` function returns a powerful `ValidationResult` object that provides multiple ways to interact with your validation results.

### Core Methods

| Method/Property | What It Does | Returns |
|-----------------|--------------|---------|
| **`print_summary()`** | Prints high-level statistics (pass/fail counts, success rate, performance) | `self` (chainable) |
| **`show_failed_rows(max_rows=5)`** | Displays sample of failed rows in console | `self` (chainable) |
| **`display_full_report()`** | Prints summary + shows failed rows (convenience method) | `self` (chainable) |
| **`save(output_dir=".", prefix="dq_results")`** | Saves enhanced CSV and summary JSON to disk | `self` (chainable) |
| **`get_enhanced_df()`** | Returns original DataFrame with `dq_validation_status` and `dq_failed_tests` columns | DataFrame |
| **`get_failed_rows()`** | Returns DataFrame of only rows that failed, with `description` column | DataFrame or None |
| **`get_passed_rows()`** | Returns DataFrame of only rows that passed all checks | DataFrame |
| **`compute_metrics()`** | Returns rule-level metrics (source_table, dimension, dq_rule, pass_rate, etc.) | DataFrame |
| **`.passed`** (property) | Boolean indicating if all checks passed | `True`/`False` |

---

## 💡 How It Works: Architecture

`dqmk` has a simple but powerful architecture designed for flexibility and scale.

1.  **Entrypoint (`validate_data`)**: When you call `validate_data`, it first inspects your DataFrame to determine its type.
2.  **Engine Detection**:
    *   If it's a `pandas.DataFrame`, it uses the **`PandasExecutor`**.
    *   If it's a `pyspark.sql.DataFrame`, it uses the **`SparkExecutor`**.
3.  **Execution**:
    *   The `PandasExecutor` uses the **DuckDB** in-memory database to run your SQL queries against the pandas DataFrame.
    *   The `SparkExecutor` uses the cluster's native **Spark SQL** engine, allowing for distributed validation on massive datasets.
4.  **Enrichment & Return**: The executor runs each check, identifies any failing rows, and enhances the original DataFrame with `dq_validation_status` and `dq_failed_tests` columns. It then bundles this enhanced DataFrame and a detailed summary into the `ValidationResult` object and returns it.

## ⚙️ Real-World Use Cases & Patterns

Here’s how you can apply `dqmk` in different scenarios.

### Interactive Analysis

Quickly understand the quality of a new dataset.

```python
import pandas as pd
from dataquality import validate_data

df = pd.read_csv("new_dataset.csv")

with validate_data(df, "contract.yaml") as result:
    result.display_full_report()
    
    if not result.passed:
        failed_df = result.get_failed_rows()
        print(failed_df['description'].value_counts())
```

---

### Production Pipeline with Quality checks

Automated data routing based on validation results:

```python
from dataquality import validate_data

raw_df = spark.read.table("staging.customer_uploads")

with validate_data(raw_df, "contracts/customers.yaml") as result:
    
    if result.passed:
        raw_df.write.mode("overwrite").saveAsTable("production.customers")
    else:
        result.get_failed_rows().write.mode("append").saveAsTable("quarantine.failed_customers")
        result.get_passed_rows().write.mode("overwrite").saveAsTable("production.customers")
```

---

### Custom Metrics & Monitoring

Track quality trends over time:

```python
from dataquality import validate_data

with validate_data(df, "contract.yaml") as result:
    
    metrics = result.compute_metrics()
    
    metrics.groupby('dimension').agg({
        'failed_row_count': 'sum',
        'pass_rate': 'mean'
    }).to_csv(f"metrics_{today}.csv")
```

