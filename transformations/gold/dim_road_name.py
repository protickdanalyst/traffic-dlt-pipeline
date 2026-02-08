import dlt
from pyspark.sql.functions import col, upper, trim, xxhash64, coalesce, lit
from config.catalog import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_road_name",
    table_properties={"quality": "gold", "layer": "gold"},
)
def dim_road_name():
    f = dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.silver_traffic")

    df = (
        f.select(
            col("region_id").cast("int").alias("region_id"),
            col("road_category_id").cast("int").alias("road_category_id"),
            upper(trim(col("road_name"))).alias("road_name_norm"),
        )
        .dropDuplicates(["region_id", "road_category_id", "road_name_norm"])
    )

    return df.withColumn(
        "road_name_sk",
        xxhash64(
            col("region_id"),
            col("road_category_id"),
            coalesce(col("road_name_norm"), lit(""))
        ).cast("long")
    )
