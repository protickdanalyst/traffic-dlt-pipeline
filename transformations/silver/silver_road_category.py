import dlt
from pyspark.sql.functions import col, upper, trim
from config.catalog import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.silver_road_category",
    table_properties={"quality": "silver", "layer": "silver"},
)
@dlt.expect_or_fail("road_category_id_not_null", "road_category_id IS NOT NULL")
def silver_road_category():
    return (
        dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_roads")
          .select(
              col("road_category_id").cast("int").alias("road_category_id"),
              upper(trim(col("road_category"))).alias("road_category"),
          )
          .dropDuplicates(["road_category_id"])
    )
