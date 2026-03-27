# ODCS Pydantic Models

This directory contains Pydantic models for the Open Data Contract Standard (ODCS).

## Directory Structure

```
models/
├── README.md              # This file
├── __init__.py            # Module exports
├── generate_models.py     # Model generation script
├── vX_Y_Z.py              # Refactored model (use this)
├── vX_Y_Z_raw.py          # Auto-generated model (reference only)
└── schemas/
    └── odcs-json-schema-vX.Y.Z.json  # Source JSON schema
```

## File Naming Convention

- `vX_Y_Z.py` - **Refactored** model with class explosion fixed (use this)
- `vX_Y_Z_raw.py` - **Auto-generated** model from JSON schema (reference only)

---

## Step 1: Generate Raw Pydantic Models

Generate raw models from JSON schema using `datamodel-code-generator`.

### Prerequisites

```bash
make install-dev
# Or: pip install datamodel-code-generator
```

### Generate

```bash
# From project root - generate all versions
make generate-models

# Or for specific version
make generate-model-version VERSION=vX.Y.Z
```

Output: `vX_Y_Z_raw.py` (~80+ classes due to schema explosion)

---

## Step 2: Fix Class Explosion with AI

The raw models have "class explosion" - redundant classes from JSON schema `oneOf`/`allOf` combinators.

Use this prompt with **GitHub Copilot** or other AI coding tools:

```
I have an auto-generated Pydantic v2 model file `vX_Y_Z_raw.py` that suffers from class explosion.

The problem is in how DataQualityOperators, DataQualityLibrary, and DataQualitySql are defined.

The JSON schema uses:
- DataQualityOperators: oneOf 8 operator classes (mustBe, mustNotBe, mustBeGreaterThan, etc.)
- DataQualityLibrary: allOf [DataQualityOperators + metric/rule/arguments fields]
- DataQualitySql: allOf [DataQualityOperators + query field]

The auto-generator creates Cartesian products: 8 operators × combinations = 17+ classes each.

Please refactor to create a new `vX_Y_Z.py` file that:

1. Create a `DataQualityBase` class with common fields (id, name, description, dimension, severity, etc.)

2. Create a `DataQualityOperatorsMixin` class with all operator fields as Optional, and a helper method `_validate_exactly_one_operator()` that validates exactly one is set.

3. Use **Pydantic discriminated unions** with `type` as the discriminator field. Create separate classes for each type:
   - `DataQualityText(DataQualityBase)` with `type: Literal["text"] = "text"`
   - `DataQualityLibrary(DataQualityBase, DataQualityOperatorsMixin)` with `type: Literal["library"] = "library"` and metric/rule/arguments fields
   - `DataQualitySql(DataQualityBase, DataQualityOperatorsMixin)` with `type: Literal["sql"]` and query field
   - `DataQualityCustom(DataQualityBase)` with `type: Literal["custom"]` and engine/implementation fields

4. Define the main `DataQuality` type as a discriminated union:
   ```python
   DataQuality = Annotated[
       Union[DataQualityText, DataQualityLibrary, DataQualitySql, DataQualityCustom],
       Field(discriminator="type"),
   ]
   ```

5. Remove all the numbered variant classes (DataQualityOperators1-8, DataQualityLibrary1-17, DataQualitySql1-17, etc.)

6. Keep all other classes unchanged.

Benefits of discriminated unions:
- Downstream code uses single `DataQuality` type
- Pydantic auto-resolves to correct subclass based on `type` field
- Type narrowing works with isinstance() and match statements
- Better IDE autocompletion per type

The goal is to reduce class count by ~50% while maintaining full schema compatibility.
Save as `vX_Y_Z.py` (without the _raw suffix).
```

Output: `vX_Y_Z.py` (~30-40 classes)

---

## Step 3: Run Tests

Validate the refactored models work correctly.

```bash
# From project root - run all tests
make test

# Or run model tests specifically
uv run pytest test/test_models.py -v
```

Expected output:
```
test/test_models.py::TestClassExplosion::test_raw_vs_refactored_reduction PASSED
Class count: ~80 -> ~30 (60%+ reduction)
```

---

## Notes

- **Do NOT modify the JSON schema** - it's the source of truth from ODCS
- **Regenerate after schema updates** - run `make generate-models`
- **Import from refactored models** - use `vX_Y_Z.py`, not `vX_Y_Z_raw.py`
- **Use discriminated unions for polymorphic types** - when JSON schema uses `if/then` or `oneOf` based on a `type` field, model it as a Pydantic discriminated union with `Field(discriminator="type")`. This gives type safety, auto-resolution, and better IDE support.
