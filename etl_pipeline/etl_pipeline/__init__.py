from dagster import Definitions, configured, load_assets_from_modules

from etl_pipeline import assets  # noqa: TID252

from etl_pipeline.config.io_manager_config import *

# from etl_pipeline.resources.clickhouse_io_manager import ClickHouseIOManager
from etl_pipeline.resources.minio_io_manager import MinIOIOManager
from etl_pipeline.resources.psql_io_manager import PostgreSQLIOManager
from etl_pipeline.resources.mysql_io_manager import MySQLIOManager
from dagster_dbt import DbtCliResource


from .constants import dbt_manifest_path, dbt_project_dir, dbt_profile_dir

import os

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,

    resources= {
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        'minio_io_manager': MinIOIOManager(MINIO_CONFIG),
        'psql_io_manager': PostgreSQLIOManager (PSQL_CONFIG),
        # 'clickhouse_io_manager': ClickHouseIOManager(CLICKHOUSE_CONFIG),
         "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir), profiles_dir=os.fspath(dbt_profile_dir))
    }
)

