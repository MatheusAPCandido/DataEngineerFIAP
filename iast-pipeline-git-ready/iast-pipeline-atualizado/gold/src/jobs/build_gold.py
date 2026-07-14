"""
Camada Gold — datasets analíticos, construídos em cima da tabela Silver
integrada (`indicador_alfabetizacao_integrado`), que já traz, por
(id_municipio, ano): resultado e meta do próprio município, da UF e do
Brasil, mais os nomes de município/UF.

Gera 5 tabelas, cobrindo os 3 exemplos citados no desafio (indicador por
município, comparação metas x resultados, evolução temporal) mais um
indicador agregado por UF e uma tabela larga pronta para dashboard:

    - gold_indicador_municipio   : indicador por município/ano
    - gold_indicador_uf          : indicador agregado por UF/ano
    - gold_comparacao_metas      : município x UF x Brasil, meta vs resultado
    - gold_evolucao_temporal     : série nacional por ano
    - gold_dashboard             : tabela larga (star schema achatado) para BI
"""

from pyspark.sql import DataFrame, SparkSession, functions as F

import config


def _read_silver(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(config.SILVER_PATHS["indicador_alfabetizacao_integrado"])


def build_gold_indicador_municipio(df: DataFrame) -> DataFrame:
    return df.select(
        "ano",
        "id_municipio",
        "nome_municipio",
        "sigla_uf",
        "nome_uf",
        "rede",
        F.col("resultado_alfabetizacao").alias("indicador_alfabetizacao_pct"),
        "meta_alfabetizacao",
        "atingiu_meta_municipio",
    )


def build_gold_indicador_uf(df: DataFrame) -> DataFrame:
    """Indicador de UF já vem calculado pela própria Base dos Dados
    (resultado_alfabetizacao_uf), então aqui só destacamos essa granularidade
    e comparamos com a meta estadual, sem reagregar a partir do município
    (evita duplo-cômputo, já que nem todo município tem meta cadastrada)."""
    return (
        df.select(
            "ano",
            "sigla_uf",
            "nome_uf",
            F.col("resultado_alfabetizacao_uf").alias("indicador_alfabetizacao_pct"),
            F.col("meta_alfabetizacao_uf").alias("meta_alfabetizacao"),
        )
        .dropDuplicates(["ano", "sigla_uf"])
        .withColumn(
            "gap_meta_pct",
            F.round(F.col("indicador_alfabetizacao_pct") - F.col("meta_alfabetizacao"), 2),
        )
        .withColumn(
            "atingiu_meta_uf",
            F.when(F.col("meta_alfabetizacao").isNull(), F.lit(None)).otherwise(
                F.col("indicador_alfabetizacao_pct") >= F.col("meta_alfabetizacao")
            ),
        )
    )


def build_gold_comparacao_metas(df: DataFrame) -> DataFrame:
    """Visão lado a lado das 3 granularidades de meta x resultado pedidas
    no desafio (município, UF, Brasil), numa linha só por (município, ano)."""
    return df.select(
        "ano",
        "id_municipio",
        "nome_municipio",
        "sigla_uf",
        F.col("resultado_alfabetizacao").alias("resultado_municipio"),
        F.col("meta_alfabetizacao").alias("meta_municipio"),
        F.col("atingiu_meta_municipio"),
        F.col("resultado_alfabetizacao_uf").alias("resultado_uf"),
        F.col("meta_alfabetizacao_uf").alias("meta_uf"),
        F.col("resultado_alfabetizacao_brasil").alias("resultado_brasil"),
        F.col("meta_alfabetizacao_brasil").alias("meta_brasil"),
    ).withColumn(
        "gap_municipio_vs_brasil",
        F.round(F.col("resultado_municipio") - F.col("resultado_brasil"), 2),
    )


def build_gold_evolucao_temporal(df: DataFrame) -> DataFrame:
    """Série nacional: média simples do resultado municipal por ano (proxy
    de evolução), mais o resultado/meta oficiais do Brasil já vindos da
    Base dos Dados, para comparação direta na mesma tabela."""
    evolucao_municipios = (
        df.groupBy("ano")
        .agg(
            F.round(F.avg("resultado_alfabetizacao"), 2).alias("media_indicador_municipios"),
            F.count("id_municipio").alias("qtd_municipios_avaliados"),
        )
    )
    evolucao_brasil = df.select(
        "ano",
        F.col("resultado_alfabetizacao_brasil").alias("resultado_brasil"),
        F.col("meta_alfabetizacao_brasil").alias("meta_brasil"),
    ).dropDuplicates(["ano"])

    return evolucao_municipios.join(evolucao_brasil, on="ano", how="left").orderBy("ano")


def build_gold_dashboard(df: DataFrame) -> DataFrame:
    """Tabela larga (star schema achatado), pronta para plugar direto num
    dashboard (QuickSight/PowerBI via Athena) sem precisar de joins adicionais."""
    return df.select(
        "ano",
        "id_municipio",
        "nome_municipio",
        "sigla_uf",
        "nome_uf",
        "rede",
        F.col("resultado_alfabetizacao").alias("indicador_municipio_pct"),
        "meta_alfabetizacao",
        "atingiu_meta_municipio",
        F.col("resultado_alfabetizacao_uf").alias("indicador_uf_pct"),
        F.col("meta_alfabetizacao_uf").alias("meta_uf"),
        F.col("resultado_alfabetizacao_brasil").alias("indicador_brasil_pct"),
        F.col("meta_alfabetizacao_brasil").alias("meta_brasil"),
        "percentual_participacao",
        "nivel_alfabetizacao",
    )


def run_gold_pipeline(spark: SparkSession) -> dict:
    df_silver = _read_silver(spark)

    resultados = {
        "gold_indicador_municipio": build_gold_indicador_municipio(df_silver),
        "gold_indicador_uf": build_gold_indicador_uf(df_silver),
        "gold_comparacao_metas": build_gold_comparacao_metas(df_silver),
        "gold_evolucao_temporal": build_gold_evolucao_temporal(df_silver),
        "gold_dashboard": build_gold_dashboard(df_silver),
    }

    for nome, df_gold in resultados.items():
        path = config.GOLD_PATHS[nome]
        writer = df_gold.write.mode("overwrite").format(config.GOLD_FORMAT)
        if "ano" in df_gold.columns:
            writer = writer.partitionBy("ano")
        writer.save(path)
        print(f"[GOLD] '{nome}' gravado em {path}")

    return resultados
