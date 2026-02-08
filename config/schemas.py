from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

ROADS_SCHEMA = StructType([
    StructField("road_id", IntegerType(), True),
    StructField("road_category_id", IntegerType(), True),
    StructField("road_category", StringType(), True),
    StructField("region_id", IntegerType(), True),
    StructField("region_name", StringType(), True),
    StructField("total_link_length_km", DoubleType(), True),
    StructField("total_link_length_miles", DoubleType(), True),
    StructField("all_motor_vehicles", DoubleType(), True),
])

TRAFFIC_SCHEMA = StructType([
    StructField("record_id", IntegerType(), True),
    StructField("count_point_id", IntegerType(), True),
    StructField("direction_of_travel", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("count_date", StringType(), True),   # raw string
    StructField("hour", IntegerType(), True),
    StructField("region_id", IntegerType(), True),
    StructField("region_name", StringType(), True),
    StructField("local_authority_name", StringType(), True),
    StructField("road_name", StringType(), True),
    StructField("road_category_id", IntegerType(), True),
    StructField("start_junction_road_name", StringType(), True),
    StructField("end_junction_road_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("link_length_km", DoubleType(), True),
    StructField("pedal_cycles", IntegerType(), True),
    StructField("two_wheeled_motor_vehicles", IntegerType(), True),
    StructField("cars_and_taxis", IntegerType(), True),
    StructField("buses_and_coaches", IntegerType(), True),
    StructField("lgv_type", IntegerType(), True),
    StructField("hgv_type", IntegerType(), True),
    StructField("ev_car", IntegerType(), True),
    StructField("ev_bike", IntegerType(), True),
])
