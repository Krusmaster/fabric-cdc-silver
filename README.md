# fabric-cdc-silver

**The problem.** A mirrored or CDC source is not a table. It is a change log with technical
columns: an operation flag, a commit sequence, a load id. Read it as a table and silver keeps
deleted rows, duplicates every updated row, and sorts by delivery order instead of business
order. Every team rediscovers this; every team writes its own slightly wrong window function.

A second problem hides behind the first: the source adds a column, the mirror picks it up, your
`select *` takes it into silver silently. Schema drift is only discovered downstream.

**What this does.**

- **Latest per key** — one row per business key, newest version wins, ordered by the source's
  commit column rather than arrival.
- **Deletes applied** — D-rows remove the key. A full initial snapshot without flags is treated as
  inserts, so the first load is not filtered away.
- **Schema contracts** — a `{column: type}` contract; drift (missing, unexpected, type changed)
  fails the run before it poisons silver.
- **Presets** for Fabric Mirroring, SQL Server CDC, Debezium and SAP Datasphere exports; or
  describe your own columns with `CdcSpec(...)`.

```python
from fabric_cdc_silver import CdcSpec, to_silver

spec = CdcSpec.datasphere(keys=["MANDT", "VBELN"])
contract = {"MANDT": "string", "VBELN": "string", "ERDAT": "date", "NETWR": "decimal(15,2)"}

silver = to_silver(spark.table("bronze.vbak_cdc"), spec, contract=contract)
```

Pair with [fabric-notebook-kit](https://github.com/Krusmaster/fabric-notebook-kit) for the
idempotent merge into the silver table:

```python
from fabric_notebook_kit import merge_by_key
merge_by_key(spark, silver, "silver.vbak", keys=spec.keys, run_id=load_id)
```

## When you do NOT need this

A **Fabric mirrored database** (including SAP Mirror via Datasphere) delivers tables as *current
state*: one row per key, already collapsed, with `__sequence_number` and `__timestamp` as
technical columns and no operation flag. There is nothing to collapse — read the table. Use
`to_silver` on the *raw* change logs upstream of that (Datasphere replication-flow Parquet,
SQL Server CDC tables, Debezium topics), not on the mirrored result. No `sap_mirror` preset is
provided on purpose.

## How you know it is right

Take a key that was changed three times and then deleted in the source. After `to_silver` it must
not exist. If it exists with its second-to-last value, you read the log as a table.

## Install

```
%pip install git+https://github.com/Krusmaster/fabric-cdc-silver
```

## Testing

Contract logic is pure Python. The Spark path is tested with local PySpark in CI and in Fabric.

## Works on

| Platform | Status |
| --- | --- |
| Fabric | yes |
| Databricks | yes — plain PySpark, no Fabric-specific calls |
| Snowflake | no (Snowpark port not written — open an issue if you need it) |
| Local Python | contracts yes; `to_silver` with local PySpark |

## License

MIT
