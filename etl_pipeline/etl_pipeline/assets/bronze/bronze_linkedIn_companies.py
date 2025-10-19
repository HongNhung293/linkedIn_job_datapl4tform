# companies            
# company_industries   
# company_specialities 
# employee_counts 

import pandas as pd
from dagster import asset, Output, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import datetime


Tables = [
    "companies",            
    "company_industries",   
    "company_specialities", 
    "employee_counts" 
]

def create_bronze_asset (table_name: str):
    @asset (
        name=f"bronze_companies_{table_name}",
        key_prefix=["bronze", "linkedin", "companies"],  
        io_manager_key="minio_io_manager",
        required_resource_keys={"mysql_io_manager"},
        compute_kind="MySQL",
        group_name="bronze"
    )

    def bronze_asset (context) -> Output[pd.DataFrame]:
        sql = f"SELECT * FROM {table_name}"
        df = context.resources.mysql_io_manager.extract_data(sql)
        context.log.info(f"Extracted {len(df)} rows from MySQL table: {table_name}")
        context.log.info (df.head (10))
        return Output(
            df,
            metadata={
                "source_table": table_name,
                "records": len(df),
            }
        )
    return bronze_asset


bronze_companies_companies = create_bronze_asset ("companies")
bronze_companies_company_industries = create_bronze_asset ("company_industries")
bronze_companies_company_specialities = create_bronze_asset ("company_specialities")



# table_partition = "employee_counts"

# @asset(
#     name=f"bronze_companies_{table_partition}",
#     key_prefix=["bronze", "linkedin", "companies"],
#     io_manager_key="minio_io_manager",
#     required_resource_keys={"mysql_io_manager"},
#     compute_kind="MySQL",
#     partitions_def=DailyPartitionsDefinition(start_date="2023-01-01"),
#     group_name="bronze"
# )
# def bronze_asset(context) -> Output[pd.DataFrame]:
#     try:
#         partition_date_str = context.asset_partition_key_for_output()
#         partition_date = datetime.datetime.strptime(partition_date_str, "%Y-%m-%d")

#         start_ts = int(partition_date.timestamp())
#         end_ts = int((partition_date + datetime.timedelta(days=1)).timestamp()) - 1

#         sql_statement = f"""
#             SELECT *
#             FROM {table_partition}
#             WHERE time_recorded BETWEEN {start_ts} AND {end_ts}
#         """
#         context.log.info(f"Running partitioned query for date: {partition_date_str}")
#     except Exception:
#         context.log.info(f"{table_partition} has no partition key! Loading full table.")
#         sql_statement = f"SELECT * FROM {table_partition}"

#     pd_data = context.resources.mysql_io_manager.extract_data(sql_statement)
#     context.log.info(pd_data.head(10))

#     return Output(
#         pd_data,
#         metadata={"table": table_partition, "record_count": len(pd_data)}
#     )


table_partition = "employee_counts"

@asset(
    name=f"bronze_companies_{table_partition}",
    key_prefix=["bronze", "linkedin", "companies"],
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    compute_kind="MySQL",
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-01-01", day_offset=1),  
    # day_offset=0 week starts from Sunday 
    # đổi =1 week starts from Monday
    group_name="bronze"
)
def bronze_asset(context) -> Output[pd.DataFrame]:
    try:
        partition_date_str = context.asset_partition_key_for_output()
        partition_date = datetime.datetime.strptime(partition_date_str, "%Y-%m-%d")

        start_ts = int(partition_date.timestamp())
        end_ts = int((partition_date + datetime.timedelta(days=7)).timestamp()) - 1

        sql_statement = f"""
            SELECT *
            FROM {table_partition}
            WHERE time_recorded BETWEEN {start_ts} AND {end_ts}
        """
        context.log.info(f"Running partitioned query for week starting: {partition_date_str}")
    except Exception:
        context.log.info(f"{table_partition} has no partition key! Loading full table.")
        sql_statement = f"SELECT * FROM {table_partition}"

    pd_data = context.resources.mysql_io_manager.extract_data(sql_statement)
    context.log.info(pd_data.head(10))

    return Output(
        pd_data,
        metadata={
            "table": table_partition,
            "record_count": len(pd_data),
            "partition": partition_date_str
        }
    )
