---
title: Using Relationship References
description: >-
  How vowl finds the target of a relationships entry, how to point at a
  property in another contract file, and how the generated check runs.
---

# Using Relationship References

A `relationships` entry of type `foreignKey` points at a property in another
schema, its **target**. vowl turns it into a check that every non-empty key
value exists in the target. The basic forms are in
[Relationships (Foreign Keys)](../../contracts.md#relationships-foreign-keys).
This page explains how vowl finds the target, and how the check runs.

## Three ways to write the target

| Notation        | Example                                                     | vowl finds the target by                       |
| --------------- | ----------------------------------------------------------- | ---------------------------------------------- |
| Shorthand       | `customers.customer_id`                                     | schema and property `name`                     |
| Fully-qualified | `/schema/customers_schema/properties/customer_id`           | schema and property `id`                       |
| External file   | `customers.yaml#/schema/<schemaId>/properties/<propertyId>` | loading the other file, then by `id` inside it |

vowl finds targets the way JSON Schema and OpenAPI find a `$ref`, following
the standard rules for relative addresses
([RFC 3986 §5](https://www.rfc-editor.org/rfc/rfc3986#section-5)).

Shorthand and fully-qualified targets that point at the same property give
the same SQL. The notation only changes how vowl finds the property. A
fully-qualified target is found by `id`, and the check uses the target's
`name`.

## Pointing at another contract file

An external target points at a property in **another contract file**:

```yaml
relationships:
  - type: foreignKey
    to: customers.yaml#/schema/customers_schema/properties/customer_id
```

- **The part after `#` must be fully-qualified.** ODCS does not allow
  shorthand after a file name, so `customers.yaml#customers.customer_id`
  fails contract validation.
- **The file is found relative to the contract that points at it.** When you
  load `orders.yaml` from a local path, an `http(s)://` URL or an `s3://`
  URI, vowl looks for `customers.yaml` next to it. A `Contract` built from an
  in-memory dict has no location, so vowl does not guess where the file is,
  and the check ends in `ERROR`.
- **External files load the same way as contracts**, so the same
  [public address rule](../../usage-patterns.md#from-git-github-or-gitlab)
  applies.

## How the check runs

Loading `customers.yaml` only tells vowl the target's schema `name` and
column. vowl never connects using a contract's `servers`. You give one adapter
per schema, keyed by schema `name`, including the target schema:

```python
validate_data(
    contract="orders.yaml",                       # loaded from a path, so vowl can find customers.yaml
    adapters={
        "orders": IbisAdapter(orders_con),         # the schema that holds the relationship
        "customers": IbisAdapter(customers_con),   # the target, keyed by its schema `name`
    },
)
```

Use the target schema's **`name`** as declared in the external file, not its
`id` and not the file name. vowl accepts it without the "no schema with that
name" warning, even though the schema is not in the contract you loaded. See
[`examples/2_multiple_sources`](https://github.com/govtech-data-practice/vowl/blob/main/examples/2_multiple_sources/multiple_sources.ipynb)
for a runnable version.

From there the check runs like any [cross-table check](how-it-works.md#where-each-check-runs):

- **Both schemas on one connection.** The check runs in that database.
- **Different connections.** vowl copies both tables into memory and runs the
  check there.
- **A table that references itself.** The check runs as a single-table check.
- **No adapter for a target in an external file.** vowl reads the target
  table through the adapter of the schema that holds the relationship, as it
  does for any
  [table outside the contract](how-it-works.md#tables-outside-the-contract).
  The check runs if that connection has the table. Otherwise it ends in
  `ERROR` with the database's "table not found" message.
- **A target vowl can't find**, such as a missing property or an external
  file with no known location to start from. The check ends in `ERROR`, with
  the reason in its message. It is named after the column, for example
  `customer_id_check`, not `<schema>_<column>_foreign_key_check`.

In every case the rest of the run goes ahead. A target schema in the same
contract is different. With `adapters={...}` it needs its own adapter like
any other schema, or `validate_data` raises
`ValueError: No adapter provided for schema(s)` before any check runs.

The failed rows hold only the columns of the schema that holds the
relationship, so they are
[annotated on that table](how-it-works.md#annotating-the-failed-rows-of-a-cross-table-check)
and don't become a residue.
