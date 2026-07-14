import json
import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

import config
from utils.logging_utils import get_logger
from utils.quality import run_quality_suite
from utils.standardize import standardize_entity

logger = get_logger(__name__)


def read_bronze(spark: SparkSession, entity_name: str) -> DataFrame:
    path = config.BRONZE_PATHS[entity_name]
    logger.info(f"Lendo Bronze de '{entity_name}' em {path}")
    reader = spark.read
    if config.BRONZE_FORMAT == "csv":
        reader = reader.option("header", True).option("inferSchema", True)
    return reader.format(config.BRONZE_FORMAT).load(path)


def write_silver(df: DataFrame, entity_name: str, partition_by: list = None) -> None:
    path = config.SILVER_PATHS[entity_name]
    logger.info(f"Gravando Silver de '{entity_name}' em {path} (formato={config.SILVER_FORMAT})")
    writer = df.write.mode("overwrite").format(config.SILVER_FORMAT)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def save_quality_report(report: dict, entity_name: str) -> None:
    """Persiste o relatório de qualidade — insumo direto para o
    monitoramento de pipelines (falhas de ingestão, alertas de erro)."""
    os.makedirs(config.QUALITY_REPORTS_PATH, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    out_path = os.path.join(
        config.QUALITY_REPORTS_PATH, f"{entity_name}_{timestamp}.json"
    )
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Relatório de qualidade salvo em {out_path} | status={report['status_geral']}")


def run_entity_job(
    spark: SparkSession,
    entity_name: str,
    partition_by: list = None,
    df_refs: dict = None,
    fail_on_alert: bool = False,
) -> DataFrame:
    """Pipeline padrão para uma entidade: Bronze -> padronização -> qualidade -> Silver."""
    entity_cfg = config.ENTITY_CONFIGS[entity_name]

    df_bronze = read_bronze(spark, entity_name)
    df_silver = standardize_entity(df_bronze, entity_cfg)

    report = run_quality_suite(df_silver, entity_name, entity_cfg, df_refs=df_refs)
    save_quality_report(report, entity_name)

    if report["status_geral"] == "ALERTA":
        logger.warning(f"Qualidade com ALERTA para '{entity_name}': {report['checks']}")
        if fail_on_alert:
            raise ValueError(f"Falha de qualidade de dados em '{entity_name}'. Ver relatório.")

    write_silver(df_silver, entity_name, partition_by=partition_by)
    return df_silver
