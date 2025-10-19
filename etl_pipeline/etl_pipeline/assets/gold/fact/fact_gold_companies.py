from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd


@asset (
    ins= {
        'silver_companies_companies': AssetIn (key_prefix=["silver", "linkedin", "companies"])
    },

    name='fact_gold_companies',
    io_manager_key="psql_io_manager",
    key_prefix=["gold", "linkedin", 'warehouse'],
    group_name="gold",
    compute_kind="Postgresql",

)
def fact_gold_companies (
    context: AssetExecutionContext,
    silver_companies_companies: pd.DataFrame
) -> Output[pd.DataFrame]:

    context.log.info (silver_companies_companies.head (10))
    total_record = len (silver_companies_companies)
    context.log.info (f'Join total {total_record} to fact_gold_companies')

    return Output (
        silver_companies_companies,
        metadata={
            "table": "fact_gold_companies",
            'record': total_record,
            'columns': list (silver_companies_companies.columns),
        }
    )
