from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd



@asset (
    ins= {
        'silver_mapping_salaries': AssetIn (key_prefix=["silver", "linkedin", "mapping"])
    },

    name='dim_gold_job_salaries',
    io_manager_key="psql_io_manager",
    key_prefix=["gold", "linkedin", 'warehouse'],
    group_name="gold",
    compute_kind="Postgresql",

)
def dim_gold_job_salaries (
    context: AssetExecutionContext,
    silver_mapping_salaries: pd.DataFrame
) -> Output[pd.DataFrame]:

    context.log.info (silver_mapping_salaries.head (10))
    total_record = len (silver_mapping_salaries)
    context.log.info (f'Transfer total {total_record} records to dim_gold_job_salaries')

    return Output (
        silver_mapping_salaries,
        metadata={
            "table": "dim_gold_job_salaries",
            'record': total_record,
            'columns': list (silver_mapping_salaries.columns),
        }
    )
