import dlt
from config.catalog import CATALOG, BRONZE_SCHEMA
from config.paths import TRAFFIC_PATH
from config.schemas import TRAFFIC_SCHEMA
from utils.dlt_utils import sanitize_columns, apply_schema, add_audit_cols

BRONZE_PROPS = {
    "quality": "bronze",
    "layer": "bronze",
    "source_format": "csv",
    "pipelines.autoOptimize.managed": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}

@dlt.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_traffic",
    table_properties=BRONZE_PROPS,
    partition_cols=["_ingest_yyyymmdd"],
)
def bronze_traffic():
    df = (
        spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.inferColumnTypes", "false")
          .option("cloudFiles.schemaEvolutionMode", "rescue")
          .option("rescuedDataColumn", "_rescued_data")
          .option("cloudFiles.includeExistingFiles", "true")
          .option("cloudFiles.validateOptions", "true")
          .option("cloudFiles.maxFilesPerTrigger", "1000")
          .option("header", "true")
          .option("multiLine", "true")
          .option("quote", '"')
          .option("escape", '"')
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .load(TRAFFIC_PATH)
    )

    df = sanitize_columns(df)
    df = apply_schema(df, TRAFFIC_SCHEMA)
    df = add_audit_cols(df)
    return df
