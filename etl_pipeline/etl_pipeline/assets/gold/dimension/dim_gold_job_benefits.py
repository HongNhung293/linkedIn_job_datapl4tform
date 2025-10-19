from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd



@asset (
    ins= {
        'silver_jobs_benefits': AssetIn (key_prefix=["silver", "linkedin", "jobs"])
    },

    name='dim_gold_job_benefits',
    io_manager_key="psql_io_manager",
    key_prefix=["gold", "linkedin", 'warehouse'],
    group_name="gold",
    compute_kind="Postgresql",

)
def dim_gold_job_benefits (
    context: AssetExecutionContext,
    silver_jobs_benefits: pd.DataFrame
) -> Output[pd.DataFrame]:

    context.log.info (silver_jobs_benefits.head (10))
    total_record = len (silver_jobs_benefits)
    context.log.info (f'Transfer total {total_record} records to dim_gold_job_benefits')

    return Output (
        silver_jobs_benefits,
        metadata={
            "table": "dim_gold_job_benefits",
            'record': total_record,
            'columns': list (silver_jobs_benefits.columns),
        }
    )
