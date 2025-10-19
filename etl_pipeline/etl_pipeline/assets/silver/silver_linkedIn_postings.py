from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, WeeklyPartitionsDefinition
import pandas as pd

from pyspark.sql.functions import regexp_replace, regexp_extract, when, col, lit
from pyspark.sql import SparkSession

# postings
@asset(
    ins={
        "bronze_postings_postings": AssetIn(key_prefix=["bronze", "linkedin", "postings"])
    },
    name="silver_postings_postings",
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "linkedin", "postings"],
    group_name="silver",
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-01-01", day_offset=1),  
    compute_kind="Spark",
)
def silver_postings_postings(
    context: AssetExecutionContext,
    bronze_postings_postings: pd.DataFrame,
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName("silver_postings_postings")
                            .master("spark://spark-master:7077")
                            .getOrCreate()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    bronze_df = spark.createDataFrame(bronze_postings_postings)
    # bronze_df.createOrReplaceTempView("postings")

    # job_id, company_name, title, description, max_salary, pay_period, location, company_id, views, med_salary, min_salary, formatted_work_type, 
    # applies, original_listed_time, remote_allowed, job_posting_url, application_url, application_type, expiry, closed_time, formatted_experience_level, 
    # skills_desc, listed_time, posting_domain, sponsored, work_type, currency, compensation_type, normalized_salary, zip_code, fips

    valid_record = bronze_df.filter(
        (col("company_name").isNotNull()) & (col("company_name") != "")
    )

    cleaned_df = valid_record.withColumn(
        "med_salary",
        when(col("med_salary") == 0, (col("max_salary") + col("min_salary")) /2)
        .otherwise(col("med_salary"))
    )

    cleaned_df = cleaned_df.dropDuplicates(cleaned_df.columns)

    pandas_df = cleaned_df.toPandas()
    context.log.info(f"Pandas shape: {pandas_df.shape}")

    context.log.info(f"Cleaned {len(pandas_df)} records to silver layer")

    return Output(
        pandas_df,
        metadata={
            "table": "postings",
            "record": len(pandas_df),
            "column": list(pandas_df.columns),
        },
    )


