# Contributing to vowl

Thank you for your interest in contributing to `vowl`! We welcome contributions from the community and are grateful for any help you can provide.

## 📋 Table of Contents

- [Branching & Workflow Strategy](#branching--workflow-strategy)
- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Release Workflow](#release-workflow)
- [Submitting Changes](#submitting-changes)
- [Reporting Issues](#reporting-issues)

## 🔀 Branching & Workflow Strategy

To maintain version consistency across the Singapore Public Sector, this repository follows a "Single Source of Truth" architecture.

1.  **Forks:** Users are encouraged to fork this repository for local deployment.
2.  **Modifications:** In accordance with Clause 3 of the project licence, any functional modifications or patches applied to a fork are part of the upstream lifecycle and are to be submitted via Merge Request to this repository prior to production deployment.
3.  **Syncing:** Please sync your fork weekly to ensure you have the latest security patches.

## 📜 Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment. Please:

- Be respectful and considerate in all interactions
- Welcome newcomers and help them get started
- Focus on constructive feedback
- Accept responsibility for your mistakes and learn from them

## 🚀 Getting Started

Before contributing, please:

1. **Clone the repository** (or fork it if you prefer)
2. **Set up the development environment** using the instructions in this document
3. **Create a new branch** for your changes

## 🛠️ Development Setup

The Makefile is the canonical source for local development commands. If a README example and a Make target ever diverge, follow the Make target.

### Prerequisites

- Install `uv`
- Install Python 3.10 or newer
- Clone or fork this repository

### Clone the Repository

```bash
git clone <your-repo-url>
cd vowl
```

### Install Development Dependencies

For standard contributor setup:

```bash
make install-dev
```

This uses the Makefile target that runs:

```bash
uv sync --group dev
```

If you need all optional dependencies as well:

```bash
make install-all
```

### Common Development Commands

Run tests:

```bash
make test
```

Run lint checks:

```bash
make lint
```

Run security scan:

```bash
make security-scan
```

Clean build artifacts:

```bash
make clean
```

## 🤝 How to Contribute

### Types of Contributions

We welcome several types of contributions:

- **Bug fixes**: Found a bug? Submit a fix!
- **New features**: Have an idea? Implement it!
- **Documentation**: Improve docs, add examples, fix typos
- **New executors**: Add support for new DataFrame types
- **New integrations**: Add platform-specific utilities
- **Test improvements**: Add test coverage or improve existing tests

## 📝 Coding Standards

### Python Style

- Follow [PEP 8](https://pep8.org/) style guidelines
- Use meaningful variable and function names
- Add docstrings to all public functions and classes
- Keep functions focused and single-purpose

### Docstring Format

Use Google-style docstrings:

```python
def validate_data(df, contract_path: str, table_name: str = None):
    """Validates a DataFrame against a data contract.
    
    Args:
        df: The DataFrame to validate (pandas or Spark).
        contract_path: Path to the YAML contract file.
        table_name: Optional override for the table name in SQL queries.
        
    Returns:
        ValidationResult: An object containing validation results and methods.
        
    Raises:
        ValueError: If the DataFrame type is not supported.
    """
```

### Type Hints

Use type hints for function signatures:

```python
from typing import Optional, List, Dict

def process_rules(rules: List[Dict], table_name: str) -> Optional[str]:
    ...
```

### Commit Messages

Write clear, descriptive commit messages:

- Use the imperative mood ("Add feature" not "Added feature")
- Keep the first line under 72 characters
- Reference issues when applicable

**Examples:**
```
Add Polars DataFrame executor support

Fix null handling in resale_price validation

Update README with new API examples (#42)
```

## 🧪 Testing

### Running Tests

Before submitting changes, ensure the automated test suite passes:

```bash
make test
```

The underlying command is:

```bash
uv run pytest test/
```

### CI Test Scope

The GitLab test job uses the `lean-ci-test` dependency group instead of the full `dev` environment:

```bash
uv sync --group lean-ci-test
```

This is intentional. CI is meant to catch core regressions quickly, but it does **not** represent the full backend matrix. In particular:

- Optional Ibis backends such as MySQL, MSSQL, and Oracle are not installed in the default CI test job
- Backend integration tests that require missing connectors are skipped rather than provisioned in CI
- Some integrations also need host-level tools or drivers beyond Python packages (for example Docker, database client libraries, or ODBC drivers)

This does **not** mean those backends are fundamentally impossible to install on GitLab CI. The main constraint is the current runner setup:

- The default job runs in `python:3.12-slim` and only installs the `lean-ci-test` group, so backend-specific Python drivers are intentionally omitted
- The backend integration tests in this repository use `testcontainers`, which means the runner also needs a usable Docker daemon or Docker-in-Docker setup
- MySQL support is backed by `mysqlclient`, which on Linux commonly needs native build prerequisites and MySQL or MariaDB client development headers
- MSSQL support is backed by `pyodbc`, but usable connections also require a registered ODBC driver; this repository's MSSQL tests currently expect FreeTDS/ODBC to be present
- Oracle support is backed by `oracledb`, which is generally installable on Linux via wheels in thin mode, but the integration tests still need Docker to start the `gvenzl/oracle-free:slim` container

If you want broader backend coverage in GitLab CI, there are workable options:

- Add a separate backend job or job matrix instead of broadening the default fast test job
- Use `uv sync --group dev` in that job so the backend extras are resolved
- Provision system dependencies in the CI image before sync, especially for MySQL and MSSQL
- Provide Docker access, either via a shell runner with Docker available or a Docker executor configured for Docker-in-Docker
- Keep Oracle and MSSQL in dedicated jobs if runtime, image size, or infrastructure setup makes them too expensive for every pipeline

If your change affects backend-specific behavior, connector-specific SQL generation, or cross-database execution paths, validate it locally with the fuller dependency set:

```bash
make install-dev
make test
```

Use `make install-all` if your change also depends on optional extras outside the default development setup.

You can still run targeted scripts or tests manually when needed:

```bash
# Run pandas demo
python test/pandas_demo.py

# Run spark demo (requires PySpark)
python test/spark_demo.py
```

### Writing Tests

When adding new features:

1. Add test cases that cover the new functionality
2. Include edge cases (null values, empty DataFrames, etc.)
3. Test both pandas and Spark implementations where applicable

### Test Data

- Use the existing `test/HDBResale.csv` for testing when possible
- For new test data, keep files small and representative
- Document any new test data files

## 🚢 Release Workflow

This section is intended for maintainers publishing `vowl` to the GitLab PyPI registry.

### Configure Upload Credentials

Create `~/.pypirc` with the GitLab registry configuration:

```ini
[gitlab]
repository = https://sgts.gitlab-dedicated.com/api/v4/projects/64873/packages/pypi
username = __token__
password = <your-gitlab-token>
```

`__token__` is the literal username required by the GitLab PyPI registry and should not be changed.

Restrict the file permissions:

```bash
chmod 600 ~/.pypirc
```

### Build and Validate the Package

```bash
make release-check
```

This target installs packaging tools, builds the distribution, and runs Twine validation.

### Upload to GitLab Package Registry

```bash
make release-upload-gitlab
```

This target runs:

```bash
python -m twine upload --repository gitlab dist/*
```

### GitLab CI Publishing

The default GitLab test pipeline is intentionally narrower than a full local development environment. It uses the `lean-ci-test` dependency group for fast regression coverage, while packaging and backend-specific validation are expected to be verified separately when a change touches those areas.

The GitLab pipeline publishes package artifacts in two cases:

- A push to `main` after a merge request is merged. Because the commit is untagged, `setuptools-scm` produces a snapshot version such as `1.2.4.dev3+gabcdef`.
- A tag such as `v1.2.3` whose commit is reachable from `main`. In that case the published package version is the clean release version `1.2.3`.

The pipeline uploads with the built-in `CI_JOB_TOKEN`; no `~/.pypirc` file is required in CI.

### Tag a Release Version

Package versions are derived from Git tags via `setuptools-scm`. For a clean release version:

```bash
make release-tag VERSION=1.2.3
git push origin v1.2.3
```

Consumers should install clean releases by pinning an exact version such as `vowl==1.2.3`. Snapshot builds from `main` remain available for internal validation, but they should be treated as pre-release artifacts rather than the default install target.

## 📤 Submitting Changes

### Merge Request Process

1. **Update your local repository** with the latest changes:
   ```bash
   git fetch origin
   git rebase origin/main
   ```

2. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes** and commit them with clear messages

4. **Push your branch:**
   ```bash
   git push origin feature/your-feature-name
   ```

5. **Open a Merge Request** against the `main` branch in GitLab

### MR Guidelines

- **Title**: Use a clear, descriptive title
- **Description**: Explain what changes you made and why
- **Testing**: Describe how you tested your changes
- **Screenshots**: Include output examples if applicable
- **Breaking Changes**: Clearly note any breaking changes

### MR Checklist

Before submitting, ensure:

- [ ] Code follows the project's style guidelines
- [ ] Documentation is updated (if applicable)
- [ ] All demos/tests pass
- [ ] Commit messages are clear and descriptive
- [ ] MR description explains the changes

## 🐛 Reporting Issues

### Bug Reports

When reporting bugs, please include:

1. **Environment details**: Python version, OS, package versions
2. **Steps to reproduce**: Minimal code example to reproduce the issue
3. **Expected behavior**: What you expected to happen
4. **Actual behavior**: What actually happened
5. **Error messages**: Full traceback if applicable

### Feature Requests

For feature requests, please describe:

1. **Use case**: Why do you need this feature?
2. **Proposed solution**: How do you envision it working?
3. **Alternatives**: Any workarounds you've considered?

## ❓ Questions?

If you have questions about contributing:

- Check existing issues and discussions
- Open a new issue with the "question" label
- Reach out to the maintainers

---

Thank you for contributing to `vowl`! Your efforts help make data quality validation better for everyone.
