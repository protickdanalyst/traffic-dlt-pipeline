import dlt
from pyspark.sql.functions import col
from config.catalog import CATALOG, SILVER_SCHEMA

dlt.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.silver_roads_scd",
    table_properties={"quality": "silver", "layer": "silver", "type": "scd2"},
)

dlt.apply_changes(
    target=f"{CATALOG}.{SILVER_SCHEMA}.silver_roads_scd",
    source="silver_roads_changes",
    keys=["road_id"],
    sequence_by=col("_sequence_ts"),
    stored_as_scd_type=2,
    ignore_null_updates=True,
    track_history_column_list=[
        "road_category_id",
        "road_category",
        "region_id",
        "total_link_length_km",
        "total_link_length_miles",
        "all_motor_vehicles",
        "row_hash",
    ],
)
