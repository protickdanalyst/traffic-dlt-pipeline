import dlt
from pyspark.sql.functions import col, coalesce, lit
from config.catalog import CATALOG, SILVER_SCHEMA

@dlt.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.silver_traffic",
    table_properties={"quality": "silver", "layer": "silver"},
)

@dlt.expect_or_fail("count_point_id_not_null", "count_point_id IS NOT NULL")
@dlt.expect_or_fail("road_category_id_not_null", "road_category_id IS NOT NULL")
@dlt.expect_or_fail("region_id_not_null", "region_id IS NOT NULL")
@dlt.expect_or_fail("hour_valid", "hour BETWEEN 0 AND 23")
@dlt.expect_or_fail("event_ts_not_null", "event_ts IS NOT NULL")

@dlt.expect("latitude_valid", "latitude IS NULL OR (latitude BETWEEN -90 AND 90)")
@dlt.expect("longitude_valid", "longitude IS NULL OR (longitude BETWEEN -180 AND 180)")

def silver_traffic():
    df = dlt.read_stream("silver_traffic_parsed")

    df = df.withColumn(
        "total_vehicles",
        (
            coalesce(col("pedal_cycles"), lit(0))
            + coalesce(col("two_wheeled_motor_vehicles"), lit(0))
            + coalesce(col("cars_and_taxis"), lit(0))
            + coalesce(col("buses_and_coaches"), lit(0))
            + coalesce(col("lgv_type"), lit(0))
            + coalesce(col("hgv_type"), lit(0))
            + coalesce(col("ev_car"), lit(0))
            + coalesce(col("ev_bike"), lit(0))
        ).cast("long")
    )

    # Dedupe on event_id (now includes measures → no silent loss)
    return df.withWatermark("event_ts", "30 days").dropDuplicates(["event_id"])
