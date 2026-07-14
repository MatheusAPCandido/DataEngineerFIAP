"""Configuração da camada Gold. Lê a Silver estruturada (indicador_alfabetizacao_integrado
+ dados_alunos + dimensões) e grava os datasets analíticos."""

# Reaproveita o mesmo bucket/prefixo base da Silver — troque para o valor real do seu bucket.
SILVER_BASE_PATH = "s3a://tech-fiap-ai/silver"
GOLD_BASE_PATH = "s3a://tech-fiap-ai/gold"

SILVER_PATHS = {
    "indicador_alfabetizacao_integrado": f"{SILVER_BASE_PATH}/indicador_alfabetizacao_integrado",
    "dados_alunos": f"{SILVER_BASE_PATH}/dados_alunos",
    "municipio": f"{SILVER_BASE_PATH}/municipio",
    "uf": f"{SILVER_BASE_PATH}/uf",
}

GOLD_PATHS = {
    "gold_indicador_municipio": f"{GOLD_BASE_PATH}/gold_indicador_municipio",
    "gold_indicador_uf": f"{GOLD_BASE_PATH}/gold_indicador_uf",
    "gold_comparacao_metas": f"{GOLD_BASE_PATH}/gold_comparacao_metas",
    "gold_evolucao_temporal": f"{GOLD_BASE_PATH}/gold_evolucao_temporal",
    "gold_dashboard": f"{GOLD_BASE_PATH}/gold_dashboard",
}

GOLD_FORMAT = "parquet"
