import dlt
from pyspark.sql.functions import col, count, sum as sum_, when, substring, expr, max as max_
from config.catalog import CATALOG, BRONZE_SCHEMA
from utils.dlt_utils import rescued_present_expr

@dlt.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_traffic_ingest_metrics",
    table_properties={"quality": "bronze", "layer": "bronze", "type": "metrics"},
)
def bronze_traffic_ingest_metrics():
    df = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_traffic")

    rescued_flag = expr(rescued_present_expr())
    rescued_sample = substring(col("_rescued_data").cast("string"), 1, 250)

    return (
        df.groupBy("_ingest_yyyymmdd", "_source_file")
          .agg(
              count("*").alias("rows_total"),
              sum_(when(rescued_flag, 1).otherwise(0)).alias("rows_with_rescued"),
              max_(when(rescued_flag, rescued_sample).otherwise(None)).alias("rescued_sample_250"),
          )
    )
