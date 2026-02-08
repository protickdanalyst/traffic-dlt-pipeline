import dlt
from pyspark.sql.functions import expr
from config.catalog import CATALOG, BRONZE_SCHEMA
from utils.dlt_utils import rescued_present_expr

@dlt.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_roads_quarantine",
    table_properties={"quality": "bronze", "layer": "bronze", "type": "quarantine"},
)
def bronze_roads_quarantine():
    df = dlt.read_stream(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_roads")
    return df.where(expr(rescued_present_expr()))
