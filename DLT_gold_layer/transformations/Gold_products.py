from pyspark import pipelines as dlt
from pyspark.sql.functions import *

# Expectations
my_rules = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_product_name": "product_name IS NOT NULL",
}
@dlt.expect_all_or_drop(my_rules)
@dlt.table(
    name = "dim_products_stg"
)
def dim_products_stg():
  df = spark.readStream.table("db_dlt_proj.silver.products")
  return df

@dlt.view()
def dim_product_stg_view():
  df = spark.readStream.table("dim_products_stg")
  return df

dlt.create_streaming_table("dim_products")

dlt.apply_changes(
  target = "dim_products",
  source = "dim_product_stg_view",
  keys = ["product_id"],
  sequence_by = "product_id",
  stored_as_scd_type=2
)