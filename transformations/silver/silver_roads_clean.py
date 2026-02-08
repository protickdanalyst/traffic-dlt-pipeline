import dlt
from pyspark.sql.functions import col, upper, trim, sha2, concat_ws, coalesce
from config.catalog import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

@dlt.view(name="silver_roads_clean")
def silver_roads_clean():
    df = (
        dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_roads")
          .select(
              col("road_id").cast("int").alias("road_id"),
              col("road_category_id").cast("int").alias("road_category_id"),
              upper(trim(col("road_category"))).alias("road_category"),
              col("region_id").cast("int").alias("region_id"),
              col("total_link_length_km").cast("double").alias("total_link_length_km"),
              col("total_link_length_miles").cast("double").alias("total_link_length_miles"),
              col("all_motor_vehicles").cast("double").alias("all_motor_vehicles"),
              col("_ingest_ts"),
              col("_source_file_mod_ts"),
          )
    )

    df = df.withColumn("_sequence_ts", coalesce(col("_source_file_mod_ts"), col("_ingest_ts")))

    return df.withColumn(
        "row_hash",
        sha2(
            concat_ws(
                "||",
                col("road_category_id").cast("string"),
                col("road_category"),
                col("region_id").cast("string"),
                col("total_link_length_km").cast("string"),
                col("total_link_length_miles").cast("string"),
                col("all_motor_vehicles").cast("string"),
            ),
            256,
        ),
    )
