"""
Configuração central da camada Silver.

IMPORTANTE: os nomes de coluna abaixo (RAW_COLUMN_MAP) são um ponto de partida
baseado nas entidades descritas no desafio (UF, Município, Metas de
Alfabetização, Dados de Alunos). Ajuste os mapeamentos para bater exatamente
com os nomes de coluna que existem nos seus arquivos/tabelas Bronze reais
(rode `df.printSchema()` na Bronze e cole aqui).
"""

from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Caminhos — Bronze já está em um bucket S3. Local + s3a:// (via hadoop-aws)
# é a forma padrão de ler S3 remotamente a partir de uma máquina local.
# TROQUE "seu-bucket" e o prefixo abaixo pelos valores reais do seu bucket.
# ---------------------------------------------------------------------------
BRONZE_BASE_PATH = "s3a://tech-fiap-ai/bronze"
# Silver pode ficar em local disk enquanto você desenvolve/testa, e depois
# apontar para o mesmo bucket (outro prefixo) quando for para "produção".
SILVER_BASE_PATH = "s3a://tech-fiap-ai/silver"
QUALITY_REPORTS_PATH = "data/silver/_quality_reports"  # fica local mesmo; são só logs de execução

BRONZE_PATHS = {
    "uf": f"{BRONZE_BASE_PATH}/UF.parquet",
    "municipio": f"{BRONZE_BASE_PATH}/Municipio.parquet",
    "meta_alfabetizacao_brasil": f"{BRONZE_BASE_PATH}/Meta alfabetizacao Brasil.parquet",
    "meta_alfabetizacao_uf": f"{BRONZE_BASE_PATH}/Meta alfabetizacao por UF.parquet",
    "meta_alfabetizacao_municipio": f"{BRONZE_BASE_PATH}/Meta alfabetizacao por municipio.parquet",
    "dados_alunos": f"{BRONZE_BASE_PATH}/Alunos.parquet",
}

SILVER_PATHS = {
    "uf": f"{SILVER_BASE_PATH}/uf",
    "municipio": f"{SILVER_BASE_PATH}/municipio",
    "meta_alfabetizacao_brasil": f"{SILVER_BASE_PATH}/meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf": f"{SILVER_BASE_PATH}/meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio": f"{SILVER_BASE_PATH}/meta_alfabetizacao_municipio",
    "dados_alunos": f"{SILVER_BASE_PATH}/dados_alunos",
    "indicador_alfabetizacao_integrado": f"{SILVER_BASE_PATH}/indicador_alfabetizacao_integrado",
}

# Formato de leitura da Bronze: "parquet", "csv" ou "delta"
BRONZE_FORMAT = "parquet"
# Formato de escrita da Silver (parquet particionado é a recomendação de custo do desafio)
SILVER_FORMAT = "parquet"

# ---------------------------------------------------------------------------
# Listas de referência para validação de consistência (governança de dados)
# ---------------------------------------------------------------------------
UFS_VALIDAS = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
}

REGIOES_VALIDAS = {"Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"}

# Faixa plausível para indicadores percentuais (ex.: % de alfabetização)
PERCENTUAL_MIN = 0.0
PERCENTUAL_MAX = 100.0

# Ano mínimo aceito nas séries históricas do indicador (Saeb/Alfabetiza Brasil)
ANO_MINIMO_VALIDO = 2019
ANO_MAXIMO_VALIDO = 2035  # cobre metas projetadas até 2030 com folga


@dataclass
class EntityConfig:
    """Configuração de padronização por entidade."""
    name: str
    # de -> para (nome final já em snake_case/minúsculo)
    rename_map: dict = field(default_factory=dict)
    # colunas que nunca podem ser nulas (linha é descartada se vier nula)
    not_null_keys: list = field(default_factory=list)
    # colunas string a normalizar (trim, upper/lower, remover acentos)
    string_columns: list = field(default_factory=list)
    # colunas numéricas a validar/castar
    numeric_columns: list = field(default_factory=list)
    # chave(s) usada(s) para deduplicação
    dedup_keys: list = field(default_factory=list)
    # entidades de meta trazem colunas largas meta_alfabetizacao_2024..2030
    # (uma por ano-alvo). É resolvida para uma única coluna meta_alfabetizacao
    # por linha, escolhendo a coluna do ano correspondente à própria linha.
    has_wide_meta_columns: bool = False


# Colunas de meta ano-a-ano como vêm da Base dos Dados (formato largo).
META_YEAR_COLUMNS = {
    2024: "meta_alfabetizacao_2024",
    2025: "meta_alfabetizacao_2025",
    2026: "meta_alfabetizacao_2026",
    2027: "meta_alfabetizacao_2027",
    2028: "meta_alfabetizacao_2028",
    2029: "meta_alfabetizacao_2029",
    2030: "meta_alfabetizacao_2030",
}

ENTITY_CONFIGS = {
    # uf.parquet / municipio.parquet no Bronze real são tabelas de
    # proficiência SAEB (ano, taxa_alfabetizacao, media_portugues,
    # proporcao_aluno_nivel_0..8) — não dimensões simples. Os jobs de
    # dimensão extraem apenas sigla_uf/nome e id_municipio/nome/sigla_uf
    # distintos a partir delas (ver jobs/silver_dimensoes.py).
    "uf": EntityConfig(
        name="uf",
        rename_map={
            "sigla_uf": "sigla_uf",
            "sigla_uf_nome": "nome_uf",
        },
        not_null_keys=["sigla_uf"],
        string_columns=["sigla_uf", "nome_uf"],
        dedup_keys=["sigla_uf"],
    ),
    "municipio": EntityConfig(
        name="municipio",
        rename_map={
            "id_municipio": "id_municipio",
            "id_municipio_nome": "nome_municipio",
            "sigla_uf": "sigla_uf",  # só existe após a correção do Bronze (v2)
        },
        not_null_keys=["id_municipio"],
        string_columns=["nome_municipio", "sigla_uf"],
        dedup_keys=["id_municipio"],
    ),
    "meta_alfabetizacao_brasil": EntityConfig(
        name="meta_alfabetizacao_brasil",
        rename_map={
            "ano": "ano",
            "rede": "rede",
            "taxa_alfabetizacao": "resultado_alfabetizacao",
            "percentual_participacao": "percentual_participacao",
            **META_YEAR_COLUMNS,
        },
        not_null_keys=["ano"],
        numeric_columns=["resultado_alfabetizacao", "percentual_participacao"]
        + list(META_YEAR_COLUMNS.values()),
        dedup_keys=["ano", "rede"],
        has_wide_meta_columns=True,
    ),
    "meta_alfabetizacao_uf": EntityConfig(
        name="meta_alfabetizacao_uf",
        rename_map={
            "sigla_uf": "sigla_uf",
            "sigla_uf_nome": "nome_uf",
            "ano": "ano",
            "rede": "rede",
            "taxa_alfabetizacao": "resultado_alfabetizacao",
            "percentual_participacao": "percentual_participacao",
            **META_YEAR_COLUMNS,
        },
        not_null_keys=["sigla_uf", "ano"],
        string_columns=["sigla_uf"],
        numeric_columns=["resultado_alfabetizacao", "percentual_participacao"]
        + list(META_YEAR_COLUMNS.values()),
        dedup_keys=["sigla_uf", "ano", "rede"],
        has_wide_meta_columns=True,
    ),
    "meta_alfabetizacao_municipio": EntityConfig(
        name="meta_alfabetizacao_municipio",
        rename_map={
            "id_municipio": "id_municipio",
            "id_municipio_nome": "nome_municipio",
            "sigla_uf": "sigla_uf",  # só existe após a correção do Bronze (v2)
            "ano": "ano",
            "rede": "rede",
            "taxa_alfabetizacao": "resultado_alfabetizacao",
            "nivel_alfabetizacao": "nivel_alfabetizacao",
            "percentual_participacao": "percentual_participacao",
            **META_YEAR_COLUMNS,
        },
        not_null_keys=["id_municipio", "ano"],
        numeric_columns=["resultado_alfabetizacao", "percentual_participacao"]
        + list(META_YEAR_COLUMNS.values()),
        dedup_keys=["id_municipio", "ano", "rede"],
        has_wide_meta_columns=True,
    ),
    "dados_alunos": EntityConfig(
        name="dados_alunos",
        rename_map={
            "id_aluno": "id_aluno",
            "id_municipio": "id_municipio",
            "id_municipio_nome": "nome_municipio",
            "sigla_uf": "sigla_uf",  # só existe após a correção do Bronze (v2)
            "id_escola": "id_escola",
            "ano": "ano",
            "caderno": "caderno",
            "serie": "serie",
            "rede": "rede",
            "presenca": "presenca",
            "preenchimento_caderno": "preenchimento_caderno",
            "alfabetizado": "alfabetizado",
            "proficiencia": "proficiencia_saeb",
            "peso_aluno": "peso_aluno",
        },
        not_null_keys=["id_municipio", "ano"],
        numeric_columns=["proficiencia_saeb", "peso_aluno"],
        dedup_keys=["id_aluno", "ano"],
    ),
}
