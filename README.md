# PySpark CI with GitHub Actions

A PySpark data-cleaning function with pytest unit tests that run
automatically on every pull request.

## What `clean_data` does

`pyspark_job.clean_data(df)` takes a DataFrame with `name` and `amount`
columns and:

- drops rows where `name` is NULL
- drops rows where `amount <= 0` (NULL amounts are dropped too)
- adds `amount_with_tax = amount * 1.20`

## Tests

`test_pyspark_job.py` runs against a local Spark session and checks that
valid rows are kept, non-positive amounts and NULL names are removed,
the tax column is correct, the output schema is as expected, and an
all-invalid input returns an empty DataFrame.

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/pytest -v
```

Needs Java 17. PySpark 3.5 supports Python up to 3.12.

## CI

`.github/workflows/ci.yml` runs on every pull request (opened, updated,
reopened) and on pushes to `main`: it sets up Java 17 and Python 3.11,
installs the pinned requirements and runs `pytest`.
