from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd

from pyspark.sql.functions import regexp_replace, regexp_extract, when, col, lit
from pyspark.sql import SparkSession

@asset (
    ins= {
        'silver_postings_postings': AssetIn (key_prefix=["silver", "linkedin", "postings"])
    },

    name='fact_gold_postings',
    io_manager_key="psql_io_manager",
    key_prefix=["gold", "linkedin", 'warehouse'],
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-01-01", day_offset=1),  
    group_name="gold",
    compute_kind="Postgresql",

)
def fact_gold_postings (
    context: AssetExecutionContext,
    silver_postings_postings: pd.DataFrame
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName("fact_gold_postings")
                            .master("spark://spark-master:7077")
                            .getOrCreate()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    df_postings = spark.createDataFrame (silver_postings_postings)
    df_postings = df_postings.drop ('max_salary', 'med_salary', 'min_salary', 'pay_period', 'currency', 'compensation_type')
    
    result = df_postings.toPandas ()

    context.log.info (result.head (10))
    total_record = len (result)
    context.log.info (f'Join total {total_record} to fact_gold_postings')

    return Output (
        result,
        metadata={
            "table": "fact_gold_postings",
            'record': total_record,
            'columns': list (result.columns),
        }
    )
