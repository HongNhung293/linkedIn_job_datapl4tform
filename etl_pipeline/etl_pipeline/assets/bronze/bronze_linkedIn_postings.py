# postings

from dagster import asset, Output, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd
import datetime

table_name = "postings"

@asset(
    name=f"bronze_postings_{table_name}",
    key_prefix=["bronze", "linkedin", "postings"],
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    compute_kind="MySQL",
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-01-01", day_offset=1),  
    group_name="bronze"
)
def bronze_asset(context) -> Output[pd.DataFrame]:
    try:
        partition_date_str = context.asset_partition_key_for_output()

        partition_date = datetime.datetime.strptime(partition_date_str, "%Y-%m-%d")

        start_ts = int(partition_date.timestamp() * 1000)
        end_ts = int((partition_date + datetime.timedelta(days=7)).timestamp() * 1000) - 1

        sql_statement = f"""
            SELECT *
            FROM {table_name}
            WHERE original_listed_time BETWEEN {start_ts} AND {end_ts}
        """
        context.log.info(f"Running partitioned query for date: {partition_date_str}")
    except Exception:
        context.log.info(f"{table_name} has no partition key! Loading full table.")
        sql_statement = f"SELECT * FROM {table_name}"

    pd_data = context.resources.mysql_io_manager.extract_data(sql_statement)
    context.log.info (pd_data.head (10))

    return Output(
        pd_data,
        metadata={"table": table_name, "record_count": len(pd_data)}
    )

# table_name = "postings"

# @asset(
#     name=f"bronze_postings_{table_name}",
#     key_prefix=["bronze", "linkedin", "postings"],
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

#         start_ts = int(partition_date.timestamp() * 1000)
#         end_ts = int((partition_date + datetime.timedelta(days=1)).timestamp() * 1000) - 1

#         sql_statement = f"""
#             SELECT *
#             FROM {table_name}
#             WHERE original_listed_time BETWEEN {start_ts} AND {end_ts}
#         """
#         context.log.info(f"Running partitioned query for date: {partition_date_str}")
#     except Exception:
#         context.log.info(f"{table_name} has no partition key! Loading full table.")
#         sql_statement = f"SELECT * FROM {table_name}"

#     pd_data = context.resources.mysql_io_manager.extract_data(sql_statement)
#     context.log.info (pd_data.head (10))

#     return Output(
#         pd_data,
#         metadata={"table": table_name, "record_count": len(pd_data)}
#     )
