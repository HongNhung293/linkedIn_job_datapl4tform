from etl_pipeline.assets import bronze
from etl_pipeline.assets import silver
from etl_pipeline.assets import gold
# from etl_pipeline.assets import warehouse
from etl_pipeline.assets import dbt

all_bronze_assets = bronze.bronze_assets
all_silver_assets = silver.silver_assets
all_gold_assets   = gold.gold_assets
all_dbt_assets    = dbt.all_assets
# all_warehouse_assets = warehouse.warehouse_assets