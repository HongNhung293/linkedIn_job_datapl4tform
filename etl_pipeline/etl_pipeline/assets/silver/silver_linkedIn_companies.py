from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd

from pyspark.sql.functions import regexp_replace, regexp_extract, when, col, lit
from pyspark.sql import SparkSession

# companies       
@asset(
    ins={
        "bronze_companies_companies": AssetIn(key_prefix=["bronze", "linkedin", "companies"])
    },
    name="silver_companies_companies",
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "linkedin", "companies"],
    group_name="silver",
    compute_kind="Spark",
)
def silver_companies_companies(
    context: AssetExecutionContext,
    bronze_companies_companies: pd.DataFrame,
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName("silver_companies_companies")
                            .master("spark://spark-master:7077")
                            .getOrCreate()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    bronze_df = spark.createDataFrame(bronze_companies_companies)
    # bronze_df.createOrReplaceTempView("companies")

    # company_id, name, description, company_size, state, country, city, zip_code, address, url

    cleaned_df = (
        bronze_df
        .withColumn("state", when(col("state").cast("string") == "0", lit("")).otherwise(col("state")))
        .withColumn("country", when(col("country").cast("string") == "0", lit("")).otherwise(col("country")))
        .withColumn("city", when(col("city").cast("string") == "0", lit("")).otherwise(col("city")))
        .withColumn("address", when(col("address").cast("string") == "0", lit("")).otherwise(col("address")))
        .withColumn("zip_code", when((col("zip_code") == "0") | col("zip_code").isNull(), lit("")).otherwise(col("zip_code")))
        .drop("company_size")
    )

    cleaned_df = cleaned_df.dropDuplicates(cleaned_df.columns)  


    pandas_df = cleaned_df.toPandas()
    context.log.info(f"Pandas shape: {pandas_df.shape}")

    context.log.info(f"Cleaned {len(pandas_df)} records to silver layer")

    return Output(
        pandas_df,
        metadata={
            "table": "companies",
            "record": len(pandas_df),
            "column": list(pandas_df.columns),
        },
    )



# company_industries
@asset (
    ins= {
        'bronze_companies_company_industries': AssetIn (key_prefix=["bronze", "linkedin", "companies"])
    },
    name='silver_companies_company_industries',
    io_manager_key='minio_io_manager',
    key_prefix=["silver", "linkedin", "companies"],
    group_name='silver',
    compute_kind='Spark'
)
def silver_companies_company_industries (
    context: AssetExecutionContext,
    bronze_companies_company_industries: pd.DataFrame
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName ('silver_companies_company_industries')
                            .master ("spark://spark-master:7077")
                            .getOrCreate ()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    bronze_companies_company_industries_df = spark.createDataFrame (bronze_companies_company_industries)
    bronze_companies_company_industries_df.createOrReplaceTempView ('company_industries')


    sql_stm = """
        SELECT
            company_id,
            CASE
                WHEN REGEXP_EXTRACT(industry, '"([^"]+)"', 1) != '' 
                THEN REGEXP_EXTRACT(industry, '"([^"]+)"', 1)
                ELSE industry
            END AS industry
        FROM company_industries
    """

    cleaned_df = spark.sql (sql_stm)

    context.log.info(f"Spark count before remove duplicate: {cleaned_df.count()}")

    cleaned_df = cleaned_df.dropDuplicates(["company_id", "industry"])

    context.log.info(f"Spark count before toPandas: {cleaned_df.count()}")

    pandas_df = cleaned_df.toPandas()
    context.log.info(f"Pandas shape: {pandas_df.shape}")


    context.log.info (f'clean {len (pandas_df)} records to silver layer')

    return Output (
        pandas_df,
        metadata={
            'table': 'company_industries',
            'record': len (pandas_df),
            'column': list (pandas_df.columns)
        }
    )  



# company_specialities
@asset (
    ins= {
        'bronze_companies_company_specialities': AssetIn (key_prefix=["bronze", "linkedin", "companies"])
    },
    name='silver_companies_company_specialities',
    io_manager_key='minio_io_manager',
    key_prefix=["silver", "linkedin", "companies"],
    group_name='silver',
    compute_kind='Spark'
)
def silver_companies_company_specialities (
    context: AssetExecutionContext,
    bronze_companies_company_specialities: pd.DataFrame
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName ('silver_companies_company_specialities')
                            .master ("spark://spark-master:7077")
                            .getOrCreate ()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    bronze_companies_company_specialities_df = spark.createDataFrame (bronze_companies_company_specialities)
    bronze_companies_company_specialities_df.createOrReplaceTempView ('company_specialities')

    sql_stm = """
        SELECT
            company_id,
            CASE
                WHEN REGEXP_EXTRACT(speciality, '"([^"]+)"', 1) != '' 
                THEN REGEXP_EXTRACT(speciality, '"([^"]+)"', 1)
                ELSE speciality
            END AS speciality
        FROM company_specialities
    """

    cleaned_df = spark.sql (sql_stm)

    context.log.info(f"Spark count before remove duplicate: {cleaned_df.count()}")

    cleaned_df = cleaned_df.dropDuplicates(list (bronze_companies_company_specialities.columns))

    context.log.info(f"Spark count before toPandas: {cleaned_df.count()}")

    pandas_df = cleaned_df.toPandas()
    context.log.info(f"Pandas shape: {pandas_df.shape}")


    context.log.info (f'clean {len (pandas_df)} records to silver layer')

    return Output (
        pandas_df,
        metadata={
            'table': 'company_industries',
            'record': len (pandas_df),
            'column': list (pandas_df.columns)
        }
    )   


# employee_counts 
@asset(
    ins={
        "bronze_companies_employee_counts": AssetIn(key_prefix=["bronze", "linkedin", "companies"])
    },
    name="silver_companies_employee_counts",
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "linkedin", "companies"],
    group_name="silver",
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-01-01", day_offset=1),  
    compute_kind="Spark",
)
def silver_companies_employee_counts(
    context: AssetExecutionContext,
    bronze_companies_employee_counts: pd.DataFrame,
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName("silver_companies_employee_counts")
                            .master("spark://spark-master:7077")
                            .getOrCreate()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    bronze_df = spark.createDataFrame(bronze_companies_employee_counts)
    bronze_df.createOrReplaceTempView("employee_counts")

    sql_stm = """
        SELECT
            company_id, employee_count, follower_count, time_recorded
        FROM employee_counts
    """

    cleaned_df = spark.sql(sql_stm)

    cleaned_df = cleaned_df.withColumn (
        'time_recorded',
        col ('time_recorded') * 1000
    )

    cleaned_df = cleaned_df.dropDuplicates(
        ["company_id", "employee_count", "follower_count", "time_recorded"]
    )

    context.log.info(f"Spark count before toPandas: {cleaned_df.count()}")

    pandas_df = cleaned_df.toPandas()
    context.log.info(f"Pandas shape: {pandas_df.shape}")

    context.log.info(f"Cleaned {len(pandas_df)} records to silver layer")

    return Output(
        pandas_df,
        metadata={
            "table": "employee_counts",
            "record": len(pandas_df),
            "column": list(pandas_df.columns),
        },
    )
