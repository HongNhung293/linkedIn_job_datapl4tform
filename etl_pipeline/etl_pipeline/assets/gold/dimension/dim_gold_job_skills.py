from dagster import asset, Output, AssetIn, AssetKey, AssetExecutionContext, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd

from pyspark.sql.functions import regexp_replace, regexp_extract, when, col, lit
from pyspark.sql import SparkSession


@asset (
    ins= {
        'silver_jobs_job_skills': AssetIn (key_prefix=["silver", "linkedin", "jobs"]),
        'silver_mapping_skills': AssetIn (key_prefix=["silver", "linkedin", "mapping"])
    },

    name='dim_gold_job_skills',
    io_manager_key="psql_io_manager",
    key_prefix=["gold", "linkedin", 'warehouse'],
    group_name="gold",
    compute_kind="Postgresql",

)
def dim_gold_job_skills (
    context: AssetExecutionContext,
    silver_jobs_job_skills: pd.DataFrame,
    silver_mapping_skills: pd.DataFrame
) -> Output[pd.DataFrame]:
    spark = (
        SparkSession.builder.appName("dim_gold_job_skills")
                            .master("spark://spark-master:7077")
                            .getOrCreate()
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    df_job_skills = spark.createDataFrame(silver_jobs_job_skills)
    df_skills = spark.createDataFrame(silver_mapping_skills)

    df_dim = (
        df_job_skills.join(df_skills, on="skill_abr", how="left")
        .select("job_id", "skill_abr", "skill_name")
        .dropDuplicates()
    )

    result_pd = df_dim.toPandas()
    total_record = len (result_pd)
    context.log.info (f'Join total {total_record} to dim_gold_job_skills')

    return Output (
        result_pd,
        metadata={
            "table": "dim_gold_job_skills",
            'record': total_record,
            'columns': list (result_pd.columns),
        }
    )
