import dlt
from pyspark.sql.functions import (
    col, to_timestamp, trim, coalesce, to_date, date_format,
    concat_ws, lpad, hour as ts_hour, minute as ts_minute, second as ts_second,
    lit, sha2, when
)
from config.catalog import CATALOG, BRONZE_SCHEMA

@dlt.view(name="silver_traffic_parsed")
def silver_traffic_parsed():
    b = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_traffic")

    s = trim(col("count_date"))

    # Multi-format timestamp parsing
    ts1 = to_timestamp(s, "M/d/yyyy H:mm")
    ts2 = to_timestamp(s, "M/d/yyyy HH:mm")
    ts3 = to_timestamp(s, "M/d/yyyy")               # date-only
    ts4 = to_timestamp(s, "yyyy-MM-dd HH:mm:ss")
    count_date_ts = coalesce(ts1, ts2, ts3, ts4)

    # If count_date_ts has real time-of-day (not 00:00:00), prefer it.
    has_time = (
        (ts_hour(count_date_ts) != lit(0))
        | (ts_minute(count_date_ts) != lit(0))
        | (ts_second(count_date_ts) != lit(0))
    )

    # Canonical timestamp from date + hour
    day_str = date_format(to_date(count_date_ts), "yyyy-MM-dd")
    hour_ts = to_timestamp(
        concat_ws(" ", day_str, lpad(col("hour").cast("string"), 2, "0")),
        "yyyy-MM-dd HH"
    )
# prefer full timestamp when it contains time
    event_ts = coalesce(when(has_time, count_date_ts),hour_ts,count_date_ts)
    # Build a stable event_id INCLUDING measures (prevents dropping real rows)
    measures_concat = concat_ws(
        "||",
        col("pedal_cycles").cast("string"),
        col("two_wheeled_motor_vehicles").cast("string"),
        col("cars_and_taxis").cast("string"),
        col("buses_and_coaches").cast("string"),
        col("lgv_type").cast("string"),
        col("hgv_type").cast("string"),
        col("ev_car").cast("string"),
        col("ev_bike").cast("string"),
    )

    event_id = sha2(
        concat_ws(
            "||",
            col("count_point_id").cast("string"),
            col("road_category_id").cast("string"),
            col("region_id").cast("string"),
            event_ts.cast("string"),
            coalesce(col("direction_of_travel"), lit("")),
            coalesce(col("road_name"), lit("")),
            measures_concat,
            # include record_id if present (helps uniqueness, but not required)
            coalesce(col("record_id").cast("string"), lit("")),
        ),
        256
    )

    return (
        b.withColumn("count_date_ts", count_date_ts)
         .withColumn("event_ts", event_ts)
         .withColumn("event_id", event_id)
    )
