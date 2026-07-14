"""
Integração das bases na Silver — o desafio pede explicitamente que a
integração aconteça nesta camada (não só a limpeza individual).

Resultado: uma tabela Silver integrada, na granularidade
(id_municipio, ano), já enriquecida com UF e metas de todas as
granularidades. Essa tabela é o insumo direto para a camada Gold
(indicador por município, comparação metas x resultados, evolução temporal).
"""

from pyspark.sql import DataFrame, SparkSession, functions as F

import config
from utils.logging_utils import get_logger
from utils.quality import check_chave_relacionamento

logger = get_logger(__name__)


def build_indicador_integrado(
    df_municipio: DataFrame,
    df_uf: DataFrame,
    df_meta_municipio: DataFrame,
    df_meta_uf: DataFrame,
    df_meta_brasil: DataFrame,
) -> DataFrame:
    # meta_alfabetizacao_municipio já traz nome_municipio/sigla_uf (vieram
    # junto na própria extração da Base dos Dados); descartamos aqui antes
    # do join para evitar colisão de nomes de coluna com df_municipio, que
    # é a fonte de referência para essas colunas.
    df_meta_municipio_join = df_meta_municipio.drop("nome_municipio", "sigla_uf")

    df = (
        df_meta_municipio_join.alias("mm")
        .join(
            df_municipio.select("id_municipio", "nome_municipio", "sigla_uf").alias("mun"),
            on="id_municipio",
            how="left",
        )
        .join(df_uf.select("sigla_uf", "nome_uf").alias("uf"), on="sigla_uf", how="left")
        .join(
            df_meta_uf.select(
                "sigla_uf",
                "ano",
                F.col("meta_alfabetizacao").alias("meta_alfabetizacao_uf"),
                F.col("resultado_alfabetizacao").alias("resultado_alfabetizacao_uf"),
            ),
            on=["sigla_uf", "ano"],
            how="left",
        )
        .join(
            df_meta_brasil.select(
                "ano",
                F.col("meta_alfabetizacao").alias("meta_alfabetizacao_brasil"),
                F.col("resultado_alfabetizacao").alias("resultado_alfabetizacao_brasil"),
            ),
            on="ano",
            how="left",
        )
    )

    # coluna analítica: município atingiu a própria meta?
    df = df.withColumn(
        "atingiu_meta_municipio",
        F.when(
            (F.col("resultado_alfabetizacao").isNotNull())
            & (F.col("meta_alfabetizacao").isNotNull()),
            F.col("resultado_alfabetizacao") >= F.col("meta_alfabetizacao"),
        ),
    )

    df = df.withColumn("_silver_processed_at", F.current_timestamp())
    return df


def run_silver_integration(
    spark: SparkSession,
    df_municipio: DataFrame,
    df_uf: DataFrame,
    df_meta_municipio: DataFrame,
    df_meta_uf: DataFrame,
    df_meta_brasil: DataFrame,
) -> DataFrame:
    df_integrado = build_indicador_integrado(
        df_municipio, df_uf, df_meta_municipio, df_meta_uf, df_meta_brasil
    )

    # validação de consistência entre tabelas pós-integração
    check = check_chave_relacionamento(df_integrado, df_municipio, "id_municipio")
    logger.info(f"Consistência município pós-integração: {check}")

    path = config.SILVER_PATHS["indicador_alfabetizacao_integrado"]
    logger.info(f"Gravando tabela Silver integrada em {path}")
    df_integrado.write.mode("overwrite").format(config.SILVER_FORMAT).partitionBy(
        "ano"
    ).save(path)

    return df_integrado
