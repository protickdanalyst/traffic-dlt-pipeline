import dlt
from pyspark.sql.functions import col, xxhash64
from config.catalog import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_region",
    table_properties={"quality": "gold", "layer": "gold"},
)
def dim_region():
    df = dlt.read(f"{CATALOG}.{SILVER_SCHEMA}.silver_region")
    return df.withColumn("region_sk", xxhash64(col("region_id")).cast("long"))
