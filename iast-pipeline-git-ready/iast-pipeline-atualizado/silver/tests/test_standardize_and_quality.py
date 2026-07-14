"""
Teste de exemplo — roda com `pytest` desde que pyspark esteja instalado
(`pip install -r requirements.txt`).

Cobre os pontos centrais da Silver: normalização de chave de município,
tratamento de UF, deduplicação e as regras de qualidade.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from pyspark.sql import SparkSession

from config import ENTITY_CONFIGS
from utils.standardize import standardize_entity, normalize_municipio_id, normalize_uf_sigla
from utils.quality import check_duplicidade, check_valores_ausentes, check_uf_valida


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("test-silver").getOrCreate()
    yield spark
    spark.stop()


def test_normalize_municipio_id(spark):
    df = spark.createDataFrame([(3550308,), (355030,)], ["id_municipio"])
    df = normalize_municipio_id(df)
    valores = [r["id_municipio"] for r in df.collect()]
    assert valores == ["3550308", "0355030"]


def test_normalize_uf_sigla(spark):
    df = spark.createDataFrame([(" sp ",), ("rj",)], ["sigla_uf"])
    df = normalize_uf_sigla(df)
    valores = [r["sigla_uf"] for r in df.collect()]
    assert valores == ["SP", "RJ"]


def test_standardize_uf_dedup_and_missing(spark):
    df = spark.createDataFrame(
        [("sp", "São Paulo ", "Sudeste"), ("sp", "São Paulo ", "Sudeste"), (None, "Erro", "Norte")],
        ["sigla_uf", "nome", "regiao"],
    )
    df_silver = standardize_entity(df, ENTITY_CONFIGS["uf"])
    # linha com sigla_uf nula deve ter sido descartada; duplicata removida
    assert df_silver.count() == 1
    assert df_silver.collect()[0]["sigla_uf"] == "SP"


def test_check_duplicidade(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    report = check_duplicidade(df, ["id"])
    assert report["duplicados"] == 1
    assert report["status"] == "ALERTA"


def test_check_valores_ausentes(spark):
    df = spark.createDataFrame([(1.0,), (None,), (3.0,)], ["resultado_alfabetizacao"])
    report = check_valores_ausentes(df, ["resultado_alfabetizacao"])
    assert report["detalhe_por_coluna"]["resultado_alfabetizacao"]["nulos"] == 1


def test_check_uf_valida(spark):
    df = spark.createDataFrame([("SP",), ("XX",)], ["sigla_uf"])
    report = check_uf_valida(df)
    assert "XX" in report["siglas_invalidas"]
    assert report["status"] == "ALERTA"
