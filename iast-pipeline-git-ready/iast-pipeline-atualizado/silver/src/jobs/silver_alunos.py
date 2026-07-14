"""
Job dos dados de alunos.

Esta é a fonte com maior volume e granularidade — no desenho híbrido do
desafio, é a candidata natural à ingestão via streaming (ex.: novas
avaliações/medições chegando quase em tempo real), enquanto UF, Município e
Metas seguem no batch. Por isso este job foi escrito para funcionar tanto
lendo um snapshot batch da Bronze quanto um micro-batch vindo do
Structured Streaming (mesma função de padronização é usada nos dois casos,
só muda o `foreachBatch`/trigger no job de streaming).
"""

from pyspark.sql import DataFrame, SparkSession

from jobs.base_job import run_entity_job
from utils.quality import run_quality_suite
from utils.standardize import standardize_entity
import config
from utils.logging_utils import get_logger

logger = get_logger(__name__)


def run_silver_alunos_batch(spark: SparkSession, df_municipio=None) -> DataFrame:
    df_refs = {"municipio": df_municipio} if df_municipio is not None else None
    return run_entity_job(
        spark, "dados_alunos", partition_by=["ano", "sigla_uf"], df_refs=df_refs
    )


def process_alunos_microbatch(df_microbatch: DataFrame, batch_id: int) -> None:
    """Callback usado em `writeStream.foreachBatch(...)` para a versão
    streaming desta mesma entidade. Aplica a mesma padronização/qualidade
    do batch e grava em append na Silver."""
    entity_cfg = config.ENTITY_CONFIGS["dados_alunos"]
    df_silver = standardize_entity(df_microbatch, entity_cfg)

    report = run_quality_suite(df_silver, "dados_alunos", entity_cfg)
    logger.info(f"[batch_id={batch_id}] qualidade dados_alunos: {report['status_geral']}")

    (
        df_silver.write.mode("append")
        .format(config.SILVER_FORMAT)
        .partitionBy("ano", "sigla_uf")
        .save(config.SILVER_PATHS["dados_alunos"])
    )
