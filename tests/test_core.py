import pytest
from fabric_cdc_silver import CdcSpec, check_contract


def test_presets_have_sane_shape():
    s = CdcSpec.sql_server_cdc(["id"])
    assert s.op_col == "__$operation" and "__$seqval" in s.columns_to_drop()
    assert CdcSpec.datasphere(["k"]).delete_values == ("D",)
    assert "_load_id" in CdcSpec.datasphere(["k"]).columns_to_drop()


def test_contract_ok():
    assert check_contract({"a": "string", "b": "int"}, {"a": "string", "b": "int"}).ok()


def test_contract_detects_all_three_kinds():
    d = check_contract({"a": "string", "c": "int"}, {"a": "int", "b": "string"})
    assert d.missing == ["b"] and d.unexpected == ["c"] and d.type_changed == [("a", "int", "string")]
    assert "missing: b" in str(d) and "a int->string" in str(d)


def test_contract_ignores_technical_columns():
    spec = CdcSpec.datasphere(["id"])
    d = check_contract({"id": "string", "_op": "string", "_commit_ts": "timestamp"}, {"id": "string"},
                       ignore=spec.columns_to_drop())
    assert d.ok()


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    return SparkSession.builder.master("local[1]").appName("t").getOrCreate()


def test_to_silver_latest_per_key_and_deletes(spark):
    from fabric_cdc_silver import to_silver
    rows = [
        ("k1", 1, "I", "v1"), ("k1", 2, "U", "v2"), ("k1", 3, "U", "v3"),   # k1 -> v3
        ("k2", 1, "I", "x"),  ("k2", 2, "D", "x"),                           # k2 deleted
        ("k3", 1, None, "snap"),                                            # initial load, no flag
    ]
    df = spark.createDataFrame(rows, ["id", "_commit_ts", "_op", "val"])
    out = to_silver(df, CdcSpec.datasphere(["id"])).orderBy("id").collect()
    assert [(r.id, r.val) for r in out] == [("k1", "v3"), ("k3", "snap")]
    assert "_op" not in out[0].asDict() and "_commit_ts" not in out[0].asDict()


def test_to_silver_fails_on_drift(spark):
    from fabric_cdc_silver import to_silver
    df = spark.createDataFrame([("k", 1, "I", "v", "extra")], ["id", "_commit_ts", "_op", "val", "new_col"])
    with pytest.raises(ValueError, match="unexpected: new_col"):
        to_silver(df, CdcSpec.datasphere(["id"]), contract={"id": "string", "val": "string"})
