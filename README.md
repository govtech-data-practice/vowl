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

### 3. Install in Editable Mode

Install the package and its dependencies. The `-e` flag allows you to make changes to the source code and have them immediately reflected.
```bash
pip install -e 
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

# 2. Run the validation
result = validate_data(df, contract_path="src/dataquality/contracts/hdb_resale.yaml")

# 3. Get a complete report printed to the console and saved to files
result.display_full_report()
```

This single command gives you a complete overview:

```
=== Validation Summary ===
Total Checks:           11
Passed:                 11
Failed:                 0
Success Rate:           100.0%
Total Rows:             1,000
Rows with Failures:     0
Total Execution Time:   145.23 ms

✅ No failed rows found!

💾 Results saved:
   - Data:    dq_results_enhanced_data.csv
   - Summary: dq_results_summary.json
```

## 🔧 The `ValidationResult` Object: Your Toolkit

The `validate_data` function returns a single, powerful `ValidationResult` object. This object is your main tool for interacting with the outcome of the validation run.

Here are the methods you can call on it:

| Method/Property | What It Does | When to Use It |
| :--- | :--- | :--- |
| `display_full_report()` | **Prints a complete summary**, shows failed rows, and saves results to files. | For interactive analysis or a quick, complete overview. The "one-button" solution. |
| `print_summary()` | Prints **only the high-level statistics** (pass/fail counts, success rate, etc.). | When you only need the aggregate statistics for a report or log. |
| `show_failed_rows(max_rows=5)` | **Displays a sample of the rows** that failed validation directly in the console. | When you want to quickly inspect the problematic data without saving anything. |
| `save()` | **Saves the summary (JSON) and the full enhanced DataFrame (CSV)** to disk. | When you need to persist the results for later analysis, auditing, or sharing. |
| `get_failed_rows()` | **Returns a DataFrame** containing *only* the rows that failed one or more checks. | For deep-dive analysis or routing bad data to a remediation pipeline. |
| `get_passed_rows()` | **Returns a DataFrame** containing *only* the rows that passed all checks. | When you want to programmatically separate and work with the clean data. |
| `.passed` (Property) | Returns a simple **`True` or `False`** indicating if all checks passed. | **Crucial for automation.** Use it in `if` statements to control your pipeline's flow. |

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

This design means you never have to worry about the underlying execution engine. Your code remains clean and engine-agnostic.

## ⚙️ Real-World Use Cases & Patterns

Here’s how you can apply `dqmk` in different scenarios.

### Pattern 1: Interactive Analysis (Data Scientist)

Quickly understand the quality of a new dataset.

```python
from dataquality import validate_data

# Exploring a new dataset
df = pd.read_csv("new_dataset.csv")
result = validate_data(df, "contract.yaml")

# Get a quick, comprehensive report
result.display_full_report()

# Now, programmatically analyze the failures
if not result.passed:
    failed_df = result.get_failed_rows()
    print("\nTop reasons for failure:")
    print(failed_df['dq_failed_tests'].explode().value_counts())
```

### Pattern 2: Automated Data Pipeline (Data Engineer)

Use `.passed` to make your pipeline robust and divert bad data.

```python
from dataquality import validate_data

# In an Airflow task or Databricks job
raw_df = spark.read.table("staging.customer_uploads")
result = validate_data(raw_df, "contracts/customers.yaml")

if result.passed:
    # Quality is perfect, promote the original data to production
    print("All checks passed. Promoting data.")
    raw_df.write.mode("overwrite").saveAsTable("production.customers")
else:
    # Quality issues found, take action
    print("Data quality issues detected. Diverting bad data for review.")
    
    # Send bad data to a quarantine table
    failed_data = result.get_failed_rows()
    failed_data.write.mode("append").saveAsTable("quarantine.failed_customers")
    
    # Optionally, promote only the clean data
    passed_data = result.get_passed_rows()
    passed_data.drop("dq_failed_tests", "dq_validation_status").write.mode("overwrite").saveAsTable("production.customers")
```

### Pattern 3: Custom Reporting

The fluent interface allows you to chain methods to create custom reports without saving files.

```python
from dataquality import validate_data

df = pd.read_csv("data.csv")
result = validate_data(df, "contract.yaml")

# I just want to see the summary and a few bad rows on my screen
print("--- Custom Validation Report ---")
result.print_summary().show_failed_rows(max_rows=3)
```
