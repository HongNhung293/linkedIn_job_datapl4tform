from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, WeeklyPartitionsDefinition
import pandas as pd


@asset(
    ins={
        "silver_companies_employee_counts": AssetIn(key_prefix=["silver", "linkedin", "companies"])
    },
    name="dim_gold_employee_counts",
    io_manager_key="psql_io_manager",
    key_prefix=["gold", "linkedin", "warehouse"],
    group_name="gold",
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-01-01", day_offset=1),  
    compute_kind="Postgresql",
)
def dim_gold_employee_counts(
    context: AssetExecutionContext,
    silver_companies_employee_counts: pd.DataFrame,
) -> Output[pd.DataFrame]:
    context.log.info(f"Pandas shape: {silver_companies_employee_counts.shape}")

    context.log.info(f"Transfer total {len(silver_companies_employee_counts)} records to silver layer")

    return Output(
        silver_companies_employee_counts,
        metadata={
            "table": "dim_gold_employee_counts",
            "record": len(silver_companies_employee_counts),
            "column": list(silver_companies_employee_counts.columns),
        },
    )
