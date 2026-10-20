"""Turn a CDC change log (mirrored table, Datasphere export, Debezium, ...) into a silver table.

A mirrored source is not a table — it is a log of changes with technical columns. Read it as a
table and you keep deleted rows, duplicate updated rows, and sort by delivery order instead of
business order. This module does "latest per key" correctly and makes schema drift fail loudly.

Pure logic lives in `CdcSpec` (testable without Spark); `to_silver()` applies it with PySpark.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence


@dataclass(frozen=True)
class CdcSpec:
    """Describes the technical columns of one CDC source. Names vary per tool; the pattern doesn't."""
    keys: Sequence[str]                     # business key columns
    order_col: str                          # commit timestamp / LSN / sequence — "which version is newest"
    op_col: str | None = None               # operation flag column (I/U/D, c/u/d, ...)
    delete_values: Sequence[str] = ("D", "d", "DELETE", "delete")
    tech_cols: Sequence[str] = ()           # extra technical columns to drop from silver
    initial_load_op: str | None = None      # value (or None) used by a full snapshot without op flag

    def columns_to_drop(self) -> list[str]:
        cols = [self.order_col, *self.tech_cols]
        if self.op_col:
            cols.append(self.op_col)
        return cols

    # Known tool presets --------------------------------------------------------
    @classmethod
    def fabric_mirroring(cls, keys: Sequence[str]) -> "CdcSpec":
        return cls(keys=keys, order_col="__rowMarker__ts", op_col="__rowMarker__",
                   delete_values=("2",), tech_cols=())

    @classmethod
    def sql_server_cdc(cls, keys: Sequence[str]) -> "CdcSpec":
        return cls(keys=keys, order_col="__$start_lsn", op_col="__$operation",
                   delete_values=("1",), tech_cols=("__$end_lsn", "__$seqval", "__$update_mask"))

    @classmethod
    def debezium(cls, keys: Sequence[str]) -> "CdcSpec":
        return cls(keys=keys, order_col="ts_ms", op_col="op", delete_values=("d",), tech_cols=("source", "transaction"))

    @classmethod
    def datasphere(cls, keys: Sequence[str]) -> "CdcSpec":
        return cls(keys=keys, order_col="_commit_ts", op_col="_op", delete_values=("D",), tech_cols=("_load_id",))


# ---- Schema contract ------------------------------------------------------------
@dataclass
class SchemaDrift:
    missing: list[str] = field(default_factory=list)      # in contract, not in data
    unexpected: list[str] = field(default_factory=list)   # in data, not in contract
    type_changed: list[tuple[str, str, str]] = field(default_factory=list)  # (col, expected, actual)

    def ok(self) -> bool:
        return not (self.missing or self.unexpected or self.type_changed)

    def __str__(self) -> str:
        parts = []
        if self.missing:
            parts.append(f"missing: {', '.join(self.missing)}")
        if self.unexpected:
            parts.append(f"unexpected: {', '.join(self.unexpected)}")
        if self.type_changed:
            parts.append("type changed: " + ", ".join(f"{c} {e}->{a}" for c, e, a in self.type_changed))
        return "; ".join(parts) or "no drift"


def check_contract(actual: dict[str, str], contract: dict[str, str], ignore: Iterable[str] = ()) -> SchemaDrift:
    """Compare {column: type} of the data against the contract. Pure; works on any schema dict."""
    ignore = set(ignore)
    drift = SchemaDrift()
    for col, typ in contract.items():
        if col not in actual:
            drift.missing.append(col)
        elif actual[col].lower() != typ.lower():
            drift.type_changed.append((col, typ, actual[col]))
    for col in actual:
        if col not in contract and col not in ignore:
            drift.unexpected.append(col)
    return drift


# ---- Spark application ----------------------------------------------------------
def to_silver(df, spec: CdcSpec, contract: dict[str, str] | None = None, fail_on_drift: bool = True):
    """Collapse a CDC log DataFrame to one row per business key (newest wins, deletes removed).

    df:        the bronze / mirrored DataFrame
    spec:      CdcSpec describing the technical columns
    contract:  optional {column: spark_type_simple_string}; drift raises (or warns) before processing
    """
    from pyspark.sql import Window, functions as F  # type: ignore

    if contract is not None:
        actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        drift = check_contract(actual, contract, ignore=spec.columns_to_drop())
        if not drift.ok():
            if fail_on_drift:
                raise ValueError(f"schema drift: {drift}")
            print(f"WARNING schema drift: {drift}")

    w = Window.partitionBy(*spec.keys).orderBy(F.col(spec.order_col).desc())
    out = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")

    if spec.op_col:
        op = F.col(spec.op_col)
        is_delete = op.isin(list(spec.delete_values))
        if spec.initial_load_op is None:
            is_delete = is_delete & op.isNotNull()      # snapshot rows without a flag are inserts
        out = out.filter(~is_delete)

    return out.drop("_rn", *[c for c in spec.columns_to_drop() if c in out.columns])
