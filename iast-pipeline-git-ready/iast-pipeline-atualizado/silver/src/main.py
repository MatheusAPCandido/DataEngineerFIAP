"""
Orquestrador da camada Silver.

Uso:
    spark-submit main.py                # roda o pipeline completo
    python main.py --local               # roda em SparkSession local (dev/teste)

Ordem de execução (respeita dependências de chave estrangeira):
  1. Dimensões: UF, Município
  2. Metas: Brasil, UF, Município
  3. Fato: Dados de Alunos (batch) — a versão streaming fica em
     jobs/silver_alunos.py::process_alunos_microbatch, plugada num job
     `writeStream` separado.
  4. Integração final: tabela única para a Gold.
"""

import argparse

from pyspark.sql import SparkSession

from jobs.silver_dimensoes import run_silver_uf, run_silver_municipio
from jobs.silver_metas import (
    run_silver_meta_brasil,
    run_silver_meta_uf,
    run_silver_meta_municipio,
)
from jobs.silver_alunos import run_silver_alunos_batch
from jobs.silver_integration import run_silver_integration
from utils.logging_utils import get_logger

logger = get_logger("main_silver")


def get_spark_session(local: bool) -> SparkSession:
    builder = SparkSession.builder.appName("silver-indicador-alfabetizacao")
    if local:
        builder = builder.master("local[*]")
        # Rodando local mas lendo/gravando em S3 remoto: precisa do conector
        # hadoop-aws (s3a://) + SDK da AWS. Baixados automaticamente via
        # Maven na primeira execução (precisa de internet nesse momento).
        # Versões abaixo combinam com Spark 3.5.x / Hadoop 3.3.4 — se você
        # usar outra versão de pyspark, ajuste a versão do hadoop-aws.
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ).config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        ).config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
    return builder.getOrCreate()


def run_pipeline(spark: SparkSession):
    logger.info("=== Iniciando pipeline Silver ===")

    logger.info("[1/4] Dimensões")
    df_uf = run_silver_uf(spark)
    df_municipio = run_silver_municipio(spark, df_uf=df_uf)

    logger.info("[2/4] Metas de alfabetização")
    df_meta_brasil = run_silver_meta_brasil(spark)
    df_meta_uf = run_silver_meta_uf(spark, df_uf=df_uf)
    df_meta_municipio = run_silver_meta_municipio(spark, df_municipio=df_municipio)

    logger.info("[3/4] Dados de alunos (batch)")
    run_silver_alunos_batch(spark, df_municipio=df_municipio)

    logger.info("[4/4] Integração das bases")
    run_silver_integration(
        spark,
        df_municipio=df_municipio,
        df_uf=df_uf,
        df_meta_municipio=df_meta_municipio,
        df_meta_uf=df_meta_uf,
        df_meta_brasil=df_meta_brasil,
    )

    logger.info("=== Pipeline Silver concluído com sucesso ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Roda com master local[*] para dev/teste")
    args = parser.parse_args()

    spark = get_spark_session(local=args.local)
    try:
        run_pipeline(spark)
    except Exception:
        logger.exception("Pipeline Silver falhou")
        raise
    finally:
        spark.stop()
