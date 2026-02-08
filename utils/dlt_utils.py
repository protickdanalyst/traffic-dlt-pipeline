from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, to_date, date_format, length

def sanitize_columns(df: DataFrame) -> DataFrame:
    for c in df.columns:
        new_c = (
            c.strip()
             .lower()
             .replace(" ", "_")
             .replace("-", "_")
        )
        while "__" in new_c:
            new_c = new_c.replace("__", "_")
        if c != new_c:
            df = df.withColumnRenamed(c, new_c)
    return df

def apply_schema(df: DataFrame, schema) -> DataFrame:
    cols = set(df.columns)
    enforced = [
        col(f.name).cast(f.dataType).alias(f.name) if f.name in cols
        else lit(None).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ]
    passthrough = [col(c) for c in df.columns if c.startswith("_")]
    return df.select(*(enforced + passthrough))

def add_audit_cols(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_ingest_ts", current_timestamp())
          .withColumn("_ingest_date", to_date(col("_ingest_ts")))
          .withColumn("_ingest_yyyymmdd", date_format(col("_ingest_ts"), "yyyyMMdd"))
          .withColumn("_source_file", col("_metadata.file_path"))
          .withColumn("_source_file_mod_ts", col("_metadata.file_modification_time"))
          .withColumn("_source_file_size", col("_metadata.file_size"))
    )

def rescued_present_expr() -> str:
    # rescued can be struct/map; cast to string and check non-trivial length
    return "_rescued_data IS NOT NULL OR length(cast(_rescued_data as string)) > 0"
