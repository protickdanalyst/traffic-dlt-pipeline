import dlt
from pyspark.sql.functions import col, max as max_
from config.catalog import CATALOG, SILVER_SCHEMA

@dlt.view(name="silver_roads_changes")
def silver_roads_changes():
    src = dlt.read_stream("silver_roads_clean").withWatermark("_sequence_ts", "90 days")

    return src.dropDuplicates(["road_id", "row_hash"])
