# Contributing to Data Quality Toolkit (dqmk)

Thank you for your interest in contributing to `dqmk`! We welcome contributions from the community and are grateful for any help you can provide.

## 📋 Table of Contents

- [Branching & Workflow Strategy](#branching--workflow-strategy)
- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
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
2. **Set up the development environment** (see [README.md](README.md#-developer-setup) for detailed instructions)
3. **Create a new branch** for your changes

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

Before submitting changes, ensure existing demos pass:

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

Thank you for contributing to `dqmk`! Your efforts help make data quality validation better for everyone. 🎉
