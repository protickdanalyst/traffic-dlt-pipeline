import dlt
from pyspark.sql.functions import col, upper, trim
from config.catalog import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.fact_traffic",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def fact_traffic():
    f  = dlt.read_stream(f"{CATALOG}.{SILVER_SCHEMA}.silver_traffic").alias("f")
    dr = dlt.read(f"{CATALOG}.{GOLD_SCHEMA}.dim_region").alias("dr")
    dc = dlt.read(f"{CATALOG}.{GOLD_SCHEMA}.dim_road_category").alias("dc")
    dn = dlt.read(f"{CATALOG}.{GOLD_SCHEMA}.dim_road_name").alias("dn")

    join_region = col("f.region_id") == col("dr.region_id")
    join_cat    = col("f.road_category_id") == col("dc.road_category_id")
    join_name = (
        (col("f.region_id") == col("dn.region_id")) &
        (col("f.road_category_id") == col("dn.road_category_id")) &
        (upper(trim(col("f.road_name"))) == col("dn.road_name_norm"))
    )

    return (
        f.join(dr, join_region, "left")
         .join(dc, join_cat, "left")
         .join(dn, join_name, "left")
         .select(
             col("f.event_ts").alias("event_ts"),
             col("f.event_id").alias("event_id"),
             col("dr.region_sk").alias("region_sk"),
             col("dc.road_category_sk").alias("road_category_sk"),
             col("dn.road_name_sk").alias("road_name_sk"),
             col("f.road_name").alias("road_name"),
             col("f.direction_of_travel").alias("direction_of_travel"),
             col("f.hour").alias("hour"),
             col("f.total_vehicles").alias("total_vehicles"),
         )
    )
