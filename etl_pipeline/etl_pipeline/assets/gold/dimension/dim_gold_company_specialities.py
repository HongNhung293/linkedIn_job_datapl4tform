from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd



@asset (
    ins= {
        'silver_companies_company_specialities': AssetIn (key_prefix=["silver", "linkedin", "companies"])
    },

    name='dim_gold_company_specialities',
    io_manager_key="psql_io_manager",
    key_prefix=["gold", "linkedin", 'warehouse'],
    group_name="gold",
    compute_kind="Postgresql",

)
def dim_gold_company_specialities (
    context: AssetExecutionContext,
    silver_companies_company_specialities: pd.DataFrame
) -> Output[pd.DataFrame]:

    context.log.info (silver_companies_company_specialities.head (10))
    total_record = len (silver_companies_company_specialities)
    context.log.info (f'Transfer total {total_record} records to dim_gold_company_specialities')

    return Output (
        silver_companies_company_specialities,
        metadata={
            "table": "dim_gold_company_specialities",
            'record': total_record,
            'columns': list (silver_companies_company_specialities.columns),
        }
    )
