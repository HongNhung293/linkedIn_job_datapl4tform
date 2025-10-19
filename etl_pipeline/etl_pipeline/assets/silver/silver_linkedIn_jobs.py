from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext
import pandas as pd

from pyspark.sql.functions import regexp_replace, regexp_extract, when, col
from pyspark.sql import SparkSession


# job_skills 
@asset (
    ins= {
        'bronze_jobs_job_skills': AssetIn (key_prefix=["bronze", "linkedin", "jobs"])
    },
    name='silver_jobs_job_skills',
    io_manager_key='minio_io_manager',
    key_prefix=["silver", "linkedin", "jobs"],
    group_name='silver',
    compute_kind='Spark'
)
def silver_jobs_job_skills (
    context: AssetExecutionContext,
    bronze_jobs_job_skills: pd.DataFrame
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName ('silver_jobs_job_skills')
                            .master ("spark://spark-master:7077")
                            .getOrCreate ()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    bronze_jobs_job_skills_df = spark.createDataFrame (bronze_jobs_job_skills)

    cleaned_df = bronze_jobs_job_skills_df.withColumn (
        "skill_abr",
        regexp_replace("skill_abr", "[\r\n]", "")
    )
    cleaned_df = cleaned_df.dropDuplicates (list(bronze_jobs_job_skills.columns))
    pandas_df = cleaned_df.toPandas ()

    context.log.info (f'clean {len (pandas_df)} records to silver layer')

    return Output (
        pandas_df,
        metadata={
            'table': 'job_skills',
            'record': len (pandas_df),
            'column': list (pandas_df.columns)
        }
    )


# benefits          
@asset (
    ins= {
        'bronze_jobs_benefits': AssetIn (key_prefix=["bronze", "linkedin", "jobs"])
    },
    name='silver_jobs_benefits',
    io_manager_key='minio_io_manager',
    key_prefix=["silver", "linkedin", "jobs"],
    group_name='silver',
    compute_kind='pandas'
)
def silver_jobs_benefits (
    context: AssetExecutionContext,
    bronze_jobs_benefits: pd.DataFrame
) -> Output[pd.DataFrame]:
    pandas_df = bronze_jobs_benefits.drop_duplicates ()

    context.log.info (f'clean {len (pandas_df)} records to silver layer')

    return Output (
        pandas_df,
        metadata={
            'table': 'benefits',
            'record': len (pandas_df),
            'column': list (pandas_df.columns)
        }
    )   



# industries
@asset (
    ins= {
        'bronze_jobs_industries': AssetIn (key_prefix=["bronze", "linkedin", "jobs"])
    },
    name='silver_jobs_industries',
    io_manager_key='minio_io_manager',
    key_prefix=["silver", "linkedin", "jobs"],
    group_name='silver',
    compute_kind='Spark'
)
def silver_jobs_industries (
    context: AssetExecutionContext,
    bronze_jobs_industries: pd.DataFrame
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName ('silver_jobs_industries')
                            .master ("spark://spark-master:7077")
                            .getOrCreate ()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    bronze_jobs_industries_df = spark.createDataFrame (bronze_jobs_industries)
    bronze_jobs_industries_df.createOrReplaceTempView ('industries')


    sql_stm = """
        SELECT
            industry_id,
            CASE
                WHEN REGEXP_EXTRACT(industry_name, '"([^"]+)"', 1) != '' 
                THEN REGEXP_EXTRACT(industry_name, '"([^"]+)"', 1)
                ELSE industry_name
            END AS industry_name
        FROM industries
    """

    cleaned_df = spark.sql (sql_stm).dropDuplicates (list(bronze_jobs_industries.columns))

    # cleaned_df = bronze_jobs_industries_df.withColumn(
    #     "industry_name",
    #     when(
    #         regexp_extract(col("industry_name"), '"([^"]+)"', 1) != "",
    #         regexp_extract(col("industry_name"), '"([^"]+)"', 1)
    #     ).otherwise(col("industry_name"))
    # )

    context.log.info(f"Spark count before toPandas: {cleaned_df.count()}")
    pandas_df = cleaned_df.toPandas()
    context.log.info(f"Pandas shape: {pandas_df.shape}")


    context.log.info (f'clean {len (pandas_df)} records to silver layer')

    return Output (
        pandas_df,
        metadata={
            'table': 'industries',
            'record': len (pandas_df),
            'column': list (pandas_df.columns)
        }
    )      


# job_industries       
@asset (
    ins= {
        'bronze_jobs_job_industries': AssetIn (key_prefix=["bronze", "linkedin", "jobs"])
    },
    name='silver_jobs_job_industries',
    io_manager_key='minio_io_manager',
    key_prefix=["silver", "linkedin", "jobs"],
    group_name='silver',
    compute_kind='pandas'
)
def silver_jobs_job_industries (
    context: AssetExecutionContext,
    bronze_jobs_job_industries: pd.DataFrame
) -> Output[pd.DataFrame]:
    pandas_df = bronze_jobs_job_industries.drop_duplicates ()

    context.log.info (f'clean {len (pandas_df)} records to silver layer')

    return Output (
        pandas_df,
        metadata={
            'table': 'job_industries',
            'record': len (pandas_df),
            'column': list (pandas_df.columns)
        }
    )