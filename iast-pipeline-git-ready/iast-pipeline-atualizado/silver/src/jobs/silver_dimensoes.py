"""Jobs das dimensões de apoio: UF e Município.
Rodam primeiro pois as demais entidades validam chave estrangeira contra elas.

IMPORTANTE: no Bronze real, `UF.parquet` e `Municipio.parquet` NÃO são
dimensões simples — são tabelas de proficiência SAEB (ano, serie, rede,
taxa_alfabetizacao, media_portugues, proporcao_aluno_nivel_0..8), com uma
linha por (uf/município, ano, serie, rede). Por isso, depois de padronizar,
extraímos apenas as colunas de identidade (sigla_uf/nome_uf ou
id_municipio/nome_municipio/sigla_uf) e tiramos distinct — o resto
(métricas de proficiência) já está coberto pelas tabelas Gold que usam
essas mesmas fontes na granularidade correta (ano/serie/rede).
"""

from pyspark.sql import SparkSession

from jobs.base_job import run_entity_job


def run_silver_uf(spark: SparkSession):
    df = run_entity_job(spark, "uf")
    return df.select("sigla_uf", "nome_uf").distinct()


def run_silver_municipio(spark: SparkSession, df_uf=None):
    df_refs = {"uf": df_uf} if df_uf is not None else None
    df = run_entity_job(spark, "municipio", df_refs=df_refs)
    colunas = ["id_municipio", "nome_municipio"]
    if "sigla_uf" in df.columns:
        colunas.append("sigla_uf")
    return df.select(*colunas).distinct()
