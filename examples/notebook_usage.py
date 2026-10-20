# In a Fabric notebook, after fabric-notebook-kit's resolve()/NotebookRun:
from fabric_cdc_silver import CdcSpec, to_silver
from fabric_notebook_kit import merge_by_key

spec = CdcSpec.datasphere(keys=["MANDT", "VBELN"])
contract = {"MANDT": "string", "VBELN": "string", "ERDAT": "date", "KUNNR": "string", "NETWR": "decimal(15,2)"}

bronze = spark.table("bronze.vbak_cdc").where(f"_load_id = '{load_id}'")
silver = to_silver(bronze, spec, contract=contract)
merge_by_key(spark, silver, "silver.vbak", keys=spec.keys, run_id=load_id)
