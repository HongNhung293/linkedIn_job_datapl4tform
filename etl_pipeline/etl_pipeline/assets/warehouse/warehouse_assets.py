from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd

from pyspark.sql.functions import regexp_replace, regexp_extract, when, col, lit
from pyspark.sql import SparkSession

    # dim_gold_job_skills,
    # dim_gold_job_industries,
    # dim_gold_job_benefits,
    # dim_gold_job_salaries,

    # dim_gold_company_industries,
    # dim_gold_company_specialities,
    
    
    # fact_gold_companies,



def create_warehouse_asset(type: str, upstream_name: str):
    @asset(
        ins={
            "input_df": AssetIn(key=AssetKey(["gold", "linkedin", type, upstream_name]))
        },
        name=upstream_name.replace(f"{type}_gold_", ""),
        key_prefix=["default", "linkedin", "warehouse"],
        io_manager_key="psql_io_manager",
        compute_kind="Postgresql",
        group_name="warehouse"
    )
    def warehouse_asset(
        context: AssetExecutionContext,
        input_df: pd.DataFrame
    ) -> Output[pd.DataFrame]:
        context.log.info(f"Extracted {len(input_df)} rows from asset: {upstream_name}")
        context.log.info(input_df.head(10))

        return Output(
            input_df,
            metadata={
                "table": upstream_name.replace(f"{type}_gold_", ""),
                "source_table": upstream_name,
                "records": len(input_df),
                "columns": list(input_df.columns),
            }
        )
    return warehouse_asset

job_skills= create_warehouse_asset ('dim', 'dim_gold_job_skills')
job_industries= create_warehouse_asset ('dim', 'dim_gold_job_industries')
job_benefits= create_warehouse_asset ('dim', 'dim_gold_job_benefits')
job_salaries= create_warehouse_asset ('dim', 'dim_gold_job_salaries')

company_industries= create_warehouse_asset ('dim', 'dim_gold_company_industries')
company_specialities= create_warehouse_asset ('dim', 'dim_gold_company_specialities')
    
companies= create_warehouse_asset ('fact', 'fact_gold_companies')


    # dim_gold_employee_counts,
    # fact_gold_postings

def create_warehouse_asset_with_partition(type: str, upstream_name: str):
    @asset(
        ins={
            "input_df": AssetIn(key=AssetKey(["gold", "linkedin", type, upstream_name]))
        },
        name=upstream_name.replace(f"{type}_gold_", ""),
        key_prefix=["default", "linkedin", "warehouse"],
        io_manager_key="psql_io_manager",
        partitions_def=WeeklyPartitionsDefinition(start_date="2023-01-01", day_offset=1),  
        compute_kind="Postgresql",
        group_name="warehouse"
    )
    def warehouse_asset(
        context: AssetExecutionContext,
        input_df: pd.DataFrame
    ) -> Output[pd.DataFrame]:
        context.log.info(f"Extracted {len(input_df)} rows from asset: {upstream_name}")
        context.log.info(input_df.head(10))

        return Output(
            input_df,
            metadata={
                "source_table": upstream_name,
                "records": len(input_df),
                "columns": list(input_df.columns),
            }
        )
    return warehouse_asset


employee_counts = create_warehouse_asset_with_partition ('dim', 'dim_gold_employee_counts')
postings = create_warehouse_asset_with_partition ('fact', 'fact_gold_postings')