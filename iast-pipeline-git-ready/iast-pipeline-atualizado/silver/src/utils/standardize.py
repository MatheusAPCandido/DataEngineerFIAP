"""
Funções genéricas de limpeza e padronização usadas por todos os jobs Silver.

Cobrem os itens pedidos no desafio para a camada Silver:
  - Limpeza de dados
  - Tratamento de valores ausentes
  - Padronização de nomes e tipos
  - Normalização de chaves
"""

import unicodedata

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType

from config import EntityConfig


def snake_case_columns(df: DataFrame) -> DataFrame:
    """Padroniza nomes de colunas: minúsculo, sem espaço, sem acento."""
    for col in df.columns:
        novo_nome = (
            unicodedata.normalize("NFKD", col)
            .encode("ascii", "ignore")
            .decode("ascii")
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )
        if novo_nome != col:
            df = df.withColumnRenamed(col, novo_nome)
    return df


def apply_rename_map(df: DataFrame, rename_map: dict) -> DataFrame:
    """Aplica o de-para de nomes definido em config.py, ignorando colunas
    que não existem no DataFrame de origem."""
    for origem, destino in rename_map.items():
        if origem in df.columns and origem != destino:
            df = df.withColumnRenamed(origem, destino)
    return df


def trim_and_normalize_strings(df: DataFrame, string_columns: list) -> DataFrame:
    """Remove espaços nas pontas e colapsa espaços duplos em colunas texto.
    Não força case aqui pois nomes próprios (município) e siglas (UF) têm
    convenções diferentes — isso é tratado nas funções específicas abaixo."""
    for c in string_columns:
        if c in df.columns:
            df = df.withColumn(
                c, F.regexp_replace(F.trim(F.col(c)), r"\s+", " ")
            )
    return df


def normalize_uf_sigla(df: DataFrame, col: str = "sigla_uf") -> DataFrame:
    """Garante sigla de UF em maiúsculas com 2 caracteres."""
    if col in df.columns:
        df = df.withColumn(col, F.upper(F.trim(F.col(col))))
    return df


def normalize_municipio_id(df: DataFrame, col: str = "id_municipio") -> DataFrame:
    """Normaliza o código IBGE do município para string de 7 dígitos
    (zero-padded), evitando problemas de join entre fontes que trazem o
    código como número (perde zero à esquerda) vs. string."""
    if col in df.columns:
        df = df.withColumn(
            col, F.lpad(F.col(col).cast("string"), 7, "0")
        )
    return df


def cast_numeric_columns(df: DataFrame, numeric_columns: list) -> DataFrame:
    """Converte colunas numéricas de forma segura (valores não conversíveis
    viram null em vez de quebrar o job) e troca vírgula decimal por ponto."""
    for c in numeric_columns:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.regexp_replace(F.col(c).cast("string"), ",", ".").cast(DoubleType()),
            )
    return df


def handle_missing_values(
    df: DataFrame, not_null_keys: list, numeric_columns: list
) -> DataFrame:
    """Estratégia de tratamento de ausentes:
      - chaves obrigatórias (not_null_keys): linha é descartada se nula,
        pois não é possível integrar/rastrear o registro sem ela;
      - colunas numéricas de indicador: nulo é mantido como null (NÃO
        imputamos com 0 ou média — isso distorceria o indicador
        educacional) mas sinalizado numa coluna de flag para a camada Gold
        decidir como tratar.
    """
    for key in not_null_keys:
        if key in df.columns:
            df = df.filter(F.col(key).isNotNull())

    for c in numeric_columns:
        if c in df.columns:
            flag_col = f"{c}_ausente"
            df = df.withColumn(flag_col, F.col(c).isNull())
    return df


def deduplicate(df: DataFrame, dedup_keys: list) -> DataFrame:
    """Remove duplicidade exata e duplicidade por chave de negócio
    (mantém o registro mais recente via coluna técnica _ingested_at,
    se existir; senão, mantém o primeiro)."""
    df = df.dropDuplicates()  # duplicidade exata linha-a-linha
    if dedup_keys and all(k in df.columns for k in dedup_keys):
        if "_ingested_at" in df.columns:
            from pyspark.sql import Window
            w = Window.partitionBy(*dedup_keys).orderBy(F.col("_ingested_at").desc())
            df = (
                df.withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )
        else:
            df = df.dropDuplicates(subset=dedup_keys)
    return df


def add_silver_audit_columns(df: DataFrame) -> DataFrame:
    """Colunas técnicas de auditoria/governança."""
    return df.withColumn("_silver_processed_at", F.current_timestamp())


def resolve_meta_for_ano(df: DataFrame) -> DataFrame:
    """As tabelas de meta vêm em formato largo: uma linha por (chave, ano)
    contendo o resultado daquele ano E as metas-alvo de 2024 a 2030, todas
    como colunas separadas (meta_alfabetizacao_2024, _2025, ...).

    Para poder comparar "resultado do ano X" com "meta do ano X" numa única
    coluna (usado na integração com Gold), resolvemos aqui qual das colunas
    largas corresponde ao próprio ano da linha, com um CASE/WHEN por ano.
    As colunas largas originais são preservadas (não removidas), então o
    detalhe ano-a-ano continua disponível para quem precisar.
    """
    from config import META_YEAR_COLUMNS

    colunas_presentes = {
        ano: col for ano, col in META_YEAR_COLUMNS.items() if col in df.columns
    }
    if not colunas_presentes:
        return df

    expressao = None
    for ano, col in colunas_presentes.items():
        if expressao is None:
            expressao = F.when(F.col("ano") == F.lit(ano), F.col(col))
        else:
            expressao = expressao.when(F.col("ano") == F.lit(ano), F.col(col))
    expressao = expressao.otherwise(F.lit(None))

    return df.withColumn("meta_alfabetizacao", expressao)


def standardize_entity(df: DataFrame, entity_cfg: EntityConfig) -> DataFrame:
    """Pipeline padrão de padronização aplicado a qualquer entidade."""
    df = snake_case_columns(df)
    df = apply_rename_map(df, entity_cfg.rename_map)
    df = trim_and_normalize_strings(df, entity_cfg.string_columns)
    if "sigla_uf" in df.columns:
        df = normalize_uf_sigla(df)
    if "id_municipio" in df.columns:
        df = normalize_municipio_id(df)
    df = cast_numeric_columns(df, entity_cfg.numeric_columns)
    if entity_cfg.has_wide_meta_columns:
        df = resolve_meta_for_ano(df)
    df = handle_missing_values(df, entity_cfg.not_null_keys, entity_cfg.numeric_columns)
    df = deduplicate(df, entity_cfg.dedup_keys)
    df = add_silver_audit_columns(df)
    return df
