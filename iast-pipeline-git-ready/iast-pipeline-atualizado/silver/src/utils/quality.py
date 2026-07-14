"""
Regras de qualidade de dados exigidas pelo desafio:
  - Verificação de duplicidade
  - Detecção de valores ausentes
  - Validação de chaves de relacionamento
  - Consistência entre tabelas

Cada função devolve um dicionário de métricas (usado tanto para decidir se o
job falha quanto para alimentar o relatório de qualidade / monitoramento).
"""

from pyspark.sql import DataFrame, functions as F

from config import PERCENTUAL_MIN, PERCENTUAL_MAX, ANO_MINIMO_VALIDO, ANO_MAXIMO_VALIDO, UFS_VALIDAS


def check_duplicidade(df: DataFrame, keys: list) -> dict:
    total = df.count()
    distintos = df.select(*keys).distinct().count() if keys else total
    duplicados = total - distintos
    return {
        "regra": "duplicidade",
        "total_registros": total,
        "registros_distintos_por_chave": distintos,
        "duplicados": duplicados,
        "status": "OK" if duplicados == 0 else "ALERTA",
    }


def check_valores_ausentes(df: DataFrame, colunas: list) -> dict:
    total = df.count()
    resultado = {}
    for c in colunas:
        if c in df.columns:
            nulos = df.filter(F.col(c).isNull()).count()
            resultado[c] = {
                "nulos": nulos,
                "percentual_nulo": round((nulos / total) * 100, 2) if total else 0,
            }
    status = "OK" if all(v["percentual_nulo"] < 5 for v in resultado.values()) else "ALERTA"
    return {"regra": "valores_ausentes", "detalhe_por_coluna": resultado, "status": status}


def check_chave_relacionamento(
    df: DataFrame, df_ref: DataFrame, chave: str, chave_ref: str = None
) -> dict:
    """Valida integridade referencial: toda chave estrangeira em df deve
    existir na tabela de referência (ex.: id_municipio de dados_alunos deve
    existir na dimensão município)."""
    chave_ref = chave_ref or chave
    valores_df = df.select(chave).distinct()
    valores_ref = df_ref.select(F.col(chave_ref).alias(chave)).distinct()
    orfaos = valores_df.join(valores_ref, on=chave, how="left_anti")
    n_orfaos = orfaos.count()
    return {
        "regra": "chave_relacionamento",
        "chave": chave,
        "valores_sem_correspondencia": n_orfaos,
        "status": "OK" if n_orfaos == 0 else "ALERTA",
    }


def check_consistencia_percentual(df: DataFrame, coluna: str) -> dict:
    """Indicadores percentuais (metas/resultados de alfabetização) devem
    estar entre 0 e 100."""
    if coluna not in df.columns:
        return {"regra": "consistencia_percentual", "coluna": coluna, "status": "N/A"}
    fora_da_faixa = df.filter(
        (F.col(coluna).isNotNull())
        & ((F.col(coluna) < PERCENTUAL_MIN) | (F.col(coluna) > PERCENTUAL_MAX))
    ).count()
    return {
        "regra": "consistencia_percentual",
        "coluna": coluna,
        "fora_da_faixa": fora_da_faixa,
        "status": "OK" if fora_da_faixa == 0 else "ALERTA",
    }


def check_consistencia_ano(df: DataFrame, coluna: str = "ano") -> dict:
    if coluna not in df.columns:
        return {"regra": "consistencia_ano", "status": "N/A"}
    fora_da_faixa = df.filter(
        (F.col(coluna) < ANO_MINIMO_VALIDO) | (F.col(coluna) > ANO_MAXIMO_VALIDO)
    ).count()
    return {
        "regra": "consistencia_ano",
        "fora_da_faixa": fora_da_faixa,
        "status": "OK" if fora_da_faixa == 0 else "ALERTA",
    }


def check_uf_valida(df: DataFrame, coluna: str = "sigla_uf") -> dict:
    if coluna not in df.columns:
        return {"regra": "uf_valida", "status": "N/A"}
    invalidas = (
        df.select(coluna).distinct().rdd.flatMap(lambda x: x).collect()
    )
    invalidas = [v for v in invalidas if v not in UFS_VALIDAS]
    return {
        "regra": "uf_valida",
        "siglas_invalidas": invalidas,
        "status": "OK" if not invalidas else "ALERTA",
    }


def run_quality_suite(df: DataFrame, entity_name: str, entity_cfg, df_refs: dict = None) -> dict:
    """Roda o conjunto de verificações aplicável a uma entidade e devolve
    um relatório único (usado pelo orquestrador e salvo para monitoramento)."""
    df_refs = df_refs or {}
    checks = []

    checks.append(check_duplicidade(df, entity_cfg.dedup_keys))
    checks.append(check_valores_ausentes(df, entity_cfg.not_null_keys + entity_cfg.numeric_columns))

    if "sigla_uf" in df.columns:
        checks.append(check_uf_valida(df))
    if "ano" in df.columns:
        checks.append(check_consistencia_ano(df))
    for col in ["meta_alfabetizacao", "resultado_alfabetizacao"]:
        if col in df.columns:
            checks.append(check_consistencia_percentual(df, col))

    # validação de chave estrangeira contra dimensões, quando aplicável
    if "id_municipio" in df.columns and "municipio" in df_refs:
        checks.append(check_chave_relacionamento(df, df_refs["municipio"], "id_municipio"))
    if "sigla_uf" in df.columns and "uf" in df_refs:
        checks.append(check_chave_relacionamento(df, df_refs["uf"], "sigla_uf"))

    status_geral = "OK" if all(c.get("status") in ("OK", "N/A") for c in checks) else "ALERTA"
    return {"entidade": entity_name, "status_geral": status_geral, "checks": checks}
