# Data Quality Library

SQL-based data quality validation with row-level failure tracking.

## Developer Setup

Follow these instructions to set up the project for local development and to run the demo script.

### 1. Clone the Repository

First, clone the repository to your local machine:

```bash
git clone <your-gitlab-repo-url>
cd dqmk
```

### 2. Create and Activate a Virtual Environment

It is highly recommended to use a virtual environment to manage project dependencies.

### 3. Install in Editable Mode

Install the package and its dependencies:
From the project root directory, run:

```bash
pip install -e .
```

### 4. Run the Demo

The repository includes a demo script to showcase the library's functionality.

Run the demo script from the project root directory:
```bash
python test/demo.py
```

You should see a validation summary printed to your console. The script will also generate `hdb_data_with_dq_results.csv` and `validation_summary_report.json` in the project's root directory.
