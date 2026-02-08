import dlt
from pyspark.sql.functions import col
from config.catalog import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA

@dlt.view(name="gold_dim_road_src")
def gold_dim_road_src():
    df = dlt.read_stream(f"{CATALOG}.{SILVER_SCHEMA}.silver_roads_scd")
    return (
        df.withColumnRenamed("__START_AT", "_start_at")
          .withColumnRenamed("__END_AT", "_end_at")
    )

dlt.create_streaming_table(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_road",
    table_properties={"quality": "gold", "layer": "gold", "type": "scd2"},
)

dlt.apply_changes(
    target=f"{CATALOG}.{GOLD_SCHEMA}.dim_road",
    source="gold_dim_road_src",
    keys=["road_id"],
    sequence_by=col("_sequence_ts"),
    stored_as_scd_type=2,
)
