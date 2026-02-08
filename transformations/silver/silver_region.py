import dlt
from pyspark.sql.functions import col, upper, trim
from config.catalog import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.silver_region",
    table_properties={"quality": "silver", "layer": "silver"},
)
@dlt.expect_or_fail("region_id_not_null", "region_id IS NOT NULL")
def silver_region():
    return (
        dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_roads")
          .select(
              col("region_id").cast("int").alias("region_id"),
              upper(trim(col("region_name"))).alias("region_name"),
          )
          .dropDuplicates(["region_id"])
    )
