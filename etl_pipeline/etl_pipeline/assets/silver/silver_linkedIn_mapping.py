from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext
import pandas as pd

from pyspark.sql.functions import regexp_replace, regexp_extract, when, col
from pyspark.sql import SparkSession


# salaries
@asset (
    ins= {
        'bronze_mapping_salaries': AssetIn (key_prefix=["bronze", "linkedin", "mapping"])
    },
    name='silver_mapping_salaries',
    io_manager_key='minio_io_manager',
    key_prefix=["silver", "linkedin", "mapping"],
    group_name='silver',
    compute_kind='Spark'
)
def silver_mapping_salaries (
    context: AssetExecutionContext,
    bronze_mapping_salaries: pd.DataFrame
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName ('silver_mapping_salaries')
                            .master ("spark://spark-master:7077")
                            .getOrCreate ()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    bronze_mapping_salaries_df = spark.createDataFrame (bronze_mapping_salaries)
    # bronze_mapping_salaries_df.createOrReplaceTempView ('salaries')

    df = bronze_mapping_salaries_df.withColumn(
        "med_salary",
        when(col("med_salary") == 0, (col("max_salary") + col("min_salary")) /2)
        .otherwise(col("med_salary"))
    )

    df = df.dropDuplicates (list (bronze_mapping_salaries.columns))

    pandas_df = df.toPandas ()

    context.log.info (f'clean {len (pandas_df)} records to silver layer')

    return Output (
        pandas_df,
        metadata={
            'table': 'skills',
            'record': len (pandas_df),
            'column': list (pandas_df.columns)
        }
    )   


# skills 
@asset (
    ins= {
        'bronze_mapping_skills': AssetIn (key_prefix=["bronze", "linkedin", "mapping"])
    },
    name='silver_mapping_skills',
    io_manager_key='minio_io_manager',
    key_prefix=["silver", "linkedin", "mapping"],
    group_name='silver',
    compute_kind='pandas'
)
def silver_mapping_skills (
    context: AssetExecutionContext,
    bronze_mapping_skills: pd.DataFrame
) -> Output[pd.DataFrame]:
    pandas_df = bronze_mapping_skills.drop_duplicates ()

    context.log.info (f'clean {len (pandas_df)} records to silver layer')

    return Output (
        pandas_df,
        metadata={
            'table': 'skills',
            'record': len (pandas_df),
            'column': list (pandas_df.columns)
        }
    )   

