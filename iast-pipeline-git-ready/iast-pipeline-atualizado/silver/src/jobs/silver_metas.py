"""Jobs das três granularidades de meta/resultado de alfabetização."""

from pyspark.sql import SparkSession

from jobs.base_job import run_entity_job


def run_silver_meta_brasil(spark: SparkSession):
    return run_entity_job(spark, "meta_alfabetizacao_brasil", partition_by=["ano"])


def run_silver_meta_uf(spark: SparkSession, df_uf=None):
    df_refs = {"uf": df_uf} if df_uf is not None else None
    return run_entity_job(
        spark, "meta_alfabetizacao_uf", partition_by=["ano"], df_refs=df_refs
    )


def run_silver_meta_municipio(spark: SparkSession, df_municipio=None):
    df_refs = {"municipio": df_municipio} if df_municipio is not None else None
    return run_entity_job(
        spark, "meta_alfabetizacao_municipio", partition_by=["ano"], df_refs=df_refs
    )
