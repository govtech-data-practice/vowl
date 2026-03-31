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

To maintain quality and consistency, this repository follows an issue-first, fork-and-PR workflow.

1.  **Open an issue first** — describe the bug, feature, or improvement before writing code. This allows maintainers to triage, discuss scope, and avoid duplicate work.
2.  **Fork & branch** — fork the repository, then create a feature branch (e.g. `feature/your-feature-name` or `fix/issue-description`) from `main`.
3.  **Submit a Pull Request** — open a PR against `main` that references the issue (e.g. `Closes #42`). CI must pass and at least one maintainer must approve before merge.
4.  **Keep your fork in sync** — pull from `upstream main` regularly to stay current with the latest changes and security patches.

## 📜 Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment. Please:

- Be respectful and considerate in all interactions
- Welcome newcomers and help them get started
- Focus on constructive feedback
- Accept responsibility for your mistakes and learn from them

## 🚀 Getting Started

Before contributing, please:

1. **Fork the repository** and clone your fork
2. **Set up the development environment** using the instructions in this document
3. **Create a new branch** for your changes

## 🛠️ Development Setup

The Makefile is the canonical source for local development commands. If a README example and a Make target ever diverge, follow the Make target.

### Prerequisites

- Install `uv`
- Install Python 3.10 or newer
- Fork and clone this repository

### Clone the Repository

```bash
git clone https://github.com/<your-username>/vowl.git
cd vowl
git remote add upstream https://github.com/govtech-data-practice/Vowl.git
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

Format code:

```bash
make format
```

Run lint checks:

```bash
make lint
```

Run type checking:

```bash
make typecheck
```

Run all code quality checks (format + lint + typecheck):

```bash
make check
```

Run security scan:

```bash
make security-scan
```

Run dependency vulnerability audit:

```bash
make security-audit
```

Run all checks and tests:

```bash
make verify
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
uv run pytest tests/
```

### CI Test Scope

The GitHub Actions CI workflow uses the `lean-ci-test` dependency group instead of the full `dev` environment:

```bash
uv sync --group lean-ci-test
```

This is intentional. CI is meant to catch core regressions quickly, but it does **not** represent the full backend matrix. In particular:

- Optional Ibis backends such as MySQL, MSSQL, and Oracle are not installed in the default CI job
- Backend integration tests that require missing connectors are skipped rather than provisioned in CI
- Some integrations also need host-level tools or drivers beyond Python packages (for example Docker, database client libraries, or ODBC drivers)

If your change affects backend-specific behavior, connector-specific SQL generation, or cross-database execution paths, validate it locally with the fuller dependency set:

```bash
make install-dev
make test
```

Use `make install-all` if your change also depends on optional extras outside the default development setup.

You can still run targeted scripts or tests manually when needed:

```bash
# Run the basic usage example
python examples/basic_usage.py

# Run a specific test file
uv run pytest tests/test_usage_patterns.py
```

### Writing Tests

When adding new features:

1. Add test cases that cover the new functionality
2. Include edge cases (null values, empty DataFrames, etc.)
3. Test both pandas and Spark implementations where applicable

### Test Data

- Use the existing `tests/hdb_resale/HDBResaleWithErrors.csv` for testing when possible
- For new test data, keep files small and representative
- Document any new test data files

## 🚢 Release Workflow

This section is intended for maintainers publishing `vowl` to PyPI.

### Build and Validate the Package

```bash
make release-check
```

This target installs packaging tools, builds the distribution, and runs Twine validation.

### CI Publishing

The GitHub Actions workflow publishes package artifacts in two cases:

- A push to `main` after a pull request is merged. Because the commit is untagged, `setuptools-scm` produces a snapshot version such as `1.2.4.dev3+gabcdef`.
- A tag such as `v1.2.3` whose commit is reachable from `main`. In that case the published package version is the clean release version `1.2.3`.

Publishing uses a GitHub Actions trusted publisher workflow; no manual API tokens are required.

### Tag a Release Version

Package versions are derived from Git tags via `setuptools-scm`. For a clean release version:

```bash
make release-tag VERSION=1.2.3
git push origin v1.2.3
```

Consumers should install clean releases by pinning an exact version such as `vowl==1.2.3`. Snapshot builds from `main` remain available for internal validation, but they should be treated as pre-release artifacts rather than the default install target.

## 📤 Submitting Changes

### Pull Request Process

1. **Sync your fork** with the latest upstream changes:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes** and commit them with clear messages

4. **Push your branch** to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

5. **Open a Pull Request** against the `main` branch, referencing the related issue (e.g. `Closes #42`)

### PR Guidelines

- **Title**: Use a clear, descriptive title
- **Description**: Explain what changes you made and why, and link the related issue
- **Testing**: Describe how you tested your changes
- **Screenshots**: Include output examples if applicable
- **Breaking Changes**: Clearly note any breaking changes

### PR Checklist

Before submitting, ensure:

- [ ] Code follows the project's style guidelines
- [ ] Documentation is updated (if applicable)
- [ ] All CI checks pass
- [ ] Commit messages are clear and descriptive
- [ ] PR description links to the related issue

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
