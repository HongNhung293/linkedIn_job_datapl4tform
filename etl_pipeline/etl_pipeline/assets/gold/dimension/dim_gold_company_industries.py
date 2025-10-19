from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd

from pyspark.sql.functions import regexp_replace, regexp_extract, when, col, lit
from pyspark.sql import SparkSession


@asset (
    ins= {
        'silver_companies_company_industries': AssetIn (key_prefix=["silver", "linkedin", "companies"]),
        'silver_jobs_industries': AssetIn (key_prefix=["silver", "linkedin", "jobs"])
    },

    name='dim_gold_company_industries',
    io_manager_key="psql_io_manager",
    key_prefix=["gold", "linkedin", 'warehouse'],
    group_name="gold",
    compute_kind="Postgresql",

)
def dim_gold_company_industries (
    context: AssetExecutionContext,
    silver_companies_company_industries: pd.DataFrame,
    silver_jobs_industries: pd.DataFrame
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName("dim_gold_company_industries")
                            .master("spark://spark-master:7077")
                            .getOrCreate()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    df_industries = spark.createDataFrame(silver_jobs_industries)
    df_company_industries = spark.createDataFrame(silver_companies_company_industries)

    df_dim = (
        df_company_industries.join(df_industries, 
                                   df_company_industries['industry'] == df_industries['industry_name'], how="left")
        .select("company_id", "industry_id", "industry_name")
        .dropDuplicates()
        .filter(
            (col("industry_id").isNotNull()) & (col("industry_id") != "") |
            (col("industry_name").isNotNull()) & (col("industry_name") != "")
    )
    )

    context.log.info (df_dim.head (10))

    result_pd = df_dim.toPandas()
    total_record = len (result_pd)
    context.log.info (f'Join total {total_record} to dim_gold_company_industries')

    return Output (
        result_pd,
        metadata={
            "table": "dim_gold_company_industries",
            'record': total_record,
            'columns': list (result_pd.columns),
        }
    )
