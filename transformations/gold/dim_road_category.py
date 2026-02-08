import dlt
from pyspark.sql.functions import col, xxhash64
from config.catalog import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_road_category",
    table_properties={"quality": "gold", "layer": "gold"},
)
def dim_road_category():
    df = dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.silver_road_category")
    return df.withColumn("road_category_sk", xxhash64(col("road_category_id")).cast("long"))
