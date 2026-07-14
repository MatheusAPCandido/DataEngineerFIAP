# -*- coding: utf-8 -*-
"""FIAP FASE 2 - CAMADA BRONZE (CORRIGIDO)

Correções aplicadas em relação à versão original:
  1. Credenciais AWS REMOVIDAS do código (usa variáveis de ambiente / Colab
     Secrets em vez de string hardcoded).
  2. Consultas de Município, Meta por Município e Alunos agora trazem
     `sigla_uf` junto com `id_municipio`, via join com o diretório de
     municípios da Base dos Dados (`br_bd_diretorios_brasil.municipio`).
     Sem isso, não era possível ligar um município/aluno a uma UF.
"""

# ---------------------------------------------------------------------------
# 1. Setup - credenciais (SEM hardcode)
# ---------------------------------------------------------------------------
# No Colab: clique no ícone de chave (🔑) na barra lateral esquerda e cadastre
# os secrets AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY antes de rodar esta célula.

import boto3
from google.colab import userdata

aws_access_key_id = userdata.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = userdata.get("AWS_SECRET_ACCESS_KEY")
region_name = "us-east-1"

boto3.setup_default_session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name,
)

s3 = boto3.client("s3")

# ---------------------------------------------------------------------------
# 2. Autenticação BigQuery (Base dos Dados)
# ---------------------------------------------------------------------------
from google.colab import auth
auth.authenticate_user()

from google.cloud import bigquery
client = bigquery.Client(project="tech-fiap-501300")

# ---------------------------------------------------------------------------
# 3. Consultas — cada uma agora traz sigla_uf quando aplicável a município
# ---------------------------------------------------------------------------
consultas = [
    {
        "nome": "UF",
        "sql": """
WITH
dicionario_serie AS (
    SELECT chave AS chave_serie, valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'serie' AND id_tabela = 'uf'
),
dicionario_rede AS (
    SELECT chave AS chave_rede, valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'rede' AND id_tabela = 'uf'
)
SELECT
    dados.ano as ano,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome,
    descricao_serie AS serie,
    descricao_rede AS rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.media_portugues as media_portugues,
    dados.proporcao_aluno_nivel_0 as proporcao_aluno_nivel_0,
    dados.proporcao_aluno_nivel_1 as proporcao_aluno_nivel_1,
    dados.proporcao_aluno_nivel_2 as proporcao_aluno_nivel_2,
    dados.proporcao_aluno_nivel_3 as proporcao_aluno_nivel_3,
    dados.proporcao_aluno_nivel_4 as proporcao_aluno_nivel_4,
    dados.proporcao_aluno_nivel_5 as proporcao_aluno_nivel_5,
    dados.proporcao_aluno_nivel_6 as proporcao_aluno_nivel_6,
    dados.proporcao_aluno_nivel_7 as proporcao_aluno_nivel_7,
    dados.proporcao_aluno_nivel_8 as proporcao_aluno_nivel_8
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf` AS dados
LEFT JOIN (SELECT DISTINCT sigla, nome FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
    ON dados.sigla_uf = diretorio_sigla_uf.sigla
LEFT JOIN `dicionario_serie` ON dados.serie = chave_serie
LEFT JOIN `dicionario_rede` ON dados.rede = chave_rede
        """,
    },
    {
        "nome": "Meta alfabetizacao Brasil",
        "sql": """
SELECT
    dados.ano as ano,
    dados.rede as rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.meta_alfabetizacao_2024 as meta_alfabetizacao_2024,
    dados.meta_alfabetizacao_2025 as meta_alfabetizacao_2025,
    dados.meta_alfabetizacao_2026 as meta_alfabetizacao_2026,
    dados.meta_alfabetizacao_2027 as meta_alfabetizacao_2027,
    dados.meta_alfabetizacao_2028 as meta_alfabetizacao_2028,
    dados.meta_alfabetizacao_2029 as meta_alfabetizacao_2029,
    dados.meta_alfabetizacao_2030 as meta_alfabetizacao_2030,
    dados.percentual_participacao as percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil` AS dados
        """,
    },
    {
        # CORRIGIDO: agora traz sigla_uf junto (a tabela de UF já tinha isso).
        "nome": "Meta alfabetizacao por UF",
        "sql": """
SELECT
    dados.ano as ano,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome,
    dados.rede as rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.meta_alfabetizacao_2024 as meta_alfabetizacao_2024,
    dados.meta_alfabetizacao_2025 as meta_alfabetizacao_2025,
    dados.meta_alfabetizacao_2026 as meta_alfabetizacao_2026,
    dados.meta_alfabetizacao_2027 as meta_alfabetizacao_2027,
    dados.meta_alfabetizacao_2028 as meta_alfabetizacao_2028,
    dados.meta_alfabetizacao_2029 as meta_alfabetizacao_2029,
    dados.meta_alfabetizacao_2030 as meta_alfabetizacao_2030,
    dados.percentual_participacao as percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf` AS dados
LEFT JOIN (SELECT DISTINCT sigla, nome FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
    ON dados.sigla_uf = diretorio_sigla_uf.sigla
        """,
    },
    {
        # CORRIGIDO: join com o diretório de município agora traz sigla_uf também.
        "nome": "Meta alfabetizacao por municipio",
        "sql": """
SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    diretorio_id_municipio.sigla_uf AS sigla_uf,
    dados.rede as rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.meta_alfabetizacao_2024 as meta_alfabetizacao_2024,
    dados.meta_alfabetizacao_2025 as meta_alfabetizacao_2025,
    dados.meta_alfabetizacao_2026 as meta_alfabetizacao_2026,
    dados.meta_alfabetizacao_2027 as meta_alfabetizacao_2027,
    dados.meta_alfabetizacao_2028 as meta_alfabetizacao_2028,
    dados.meta_alfabetizacao_2029 as meta_alfabetizacao_2029,
    dados.meta_alfabetizacao_2030 as meta_alfabetizacao_2030,
    dados.nivel_alfabetizacao as nivel_alfabetizacao,
    dados.percentual_participacao as percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio, nome, sigla_uf FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
        """,
    },
    {
        # CORRIGIDO: join com o diretório de município agora traz sigla_uf também.
        "nome": "Municipio",
        "sql": """
WITH
dicionario_serie AS (
    SELECT chave AS chave_serie, valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'serie' AND id_tabela = 'municipio'
),
dicionario_rede AS (
    SELECT chave AS chave_rede, valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'rede' AND id_tabela = 'municipio'
)
SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    diretorio_id_municipio.sigla_uf AS sigla_uf,
    descricao_serie AS serie,
    descricao_rede AS rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.media_portugues as media_portugues,
    dados.proporcao_aluno_nivel_0 as proporcao_aluno_nivel_0,
    dados.proporcao_aluno_nivel_1 as proporcao_aluno_nivel_1,
    dados.proporcao_aluno_nivel_2 as proporcao_aluno_nivel_2,
    dados.proporcao_aluno_nivel_3 as proporcao_aluno_nivel_3,
    dados.proporcao_aluno_nivel_4 as proporcao_aluno_nivel_4,
    dados.proporcao_aluno_nivel_5 as proporcao_aluno_nivel_5,
    dados.proporcao_aluno_nivel_6 as proporcao_aluno_nivel_6,
    dados.proporcao_aluno_nivel_7 as proporcao_aluno_nivel_7,
    dados.proporcao_aluno_nivel_8 as proporcao_aluno_nivel_8
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio, nome, sigla_uf FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN `dicionario_serie` ON dados.serie = chave_serie
LEFT JOIN `dicionario_rede` ON dados.rede = chave_rede
        """,
    },
    {
        # CORRIGIDO: join com o diretório de município agora traz sigla_uf também.
        "nome": "Alunos",
        "sql": """
WITH
dicionario_serie AS (
    SELECT chave AS chave_serie, valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'serie' AND id_tabela = 'alunos'
),
dicionario_rede AS (
    SELECT chave AS chave_rede, valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'rede' AND id_tabela = 'alunos'
),
dicionario_presenca AS (
    SELECT chave AS chave_presenca, valor AS descricao_presenca
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'presenca' AND id_tabela = 'alunos'
),
dicionario_preenchimento_caderno AS (
    SELECT chave AS chave_preenchimento_caderno, valor AS descricao_preenchimento_caderno
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'preenchimento_caderno' AND id_tabela = 'alunos'
),
dicionario_alfabetizado AS (
    SELECT chave AS chave_alfabetizado, valor AS descricao_alfabetizado
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'alfabetizado' AND id_tabela = 'alunos'
)
SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    diretorio_id_municipio.sigla_uf AS sigla_uf,
    dados.id_escola as id_escola,
    dados.id_aluno as id_aluno,
    dados.caderno as caderno,
    descricao_serie AS serie,
    descricao_rede AS rede,
    descricao_presenca AS presenca,
    descricao_preenchimento_caderno AS preenchimento_caderno,
    descricao_alfabetizado AS alfabetizado,
    dados.proficiencia as proficiencia,
    dados.peso_aluno as peso_aluno
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio, nome, sigla_uf FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN `dicionario_serie` ON dados.serie = chave_serie
LEFT JOIN `dicionario_rede` ON dados.rede = chave_rede
LEFT JOIN `dicionario_presenca` ON dados.presenca = chave_presenca
LEFT JOIN `dicionario_preenchimento_caderno` ON dados.preenchimento_caderno = chave_preenchimento_caderno
LEFT JOIN `dicionario_alfabetizado` ON dados.alfabetizado = chave_alfabetizado
        """,
    },
]

# ---------------------------------------------------------------------------
# 4. Execução: roda cada consulta e sobe pro S3 (bucket/prefixo iguais aos originais)
# ---------------------------------------------------------------------------
for consulta in consultas:
    print(f"Executando {consulta['nome']}...")
    df = client.query(consulta["sql"]).to_dataframe()
    nome_arquivo = f"{consulta['nome']}.parquet"
    df.to_parquet(nome_arquivo, index=False)
    s3.upload_file(nome_arquivo, "tech-fiap-ai", f"bronze/{nome_arquivo}")
    print(f"{nome_arquivo} enviado com sucesso ({len(df)} linhas, colunas: {list(df.columns)})")

print("\nBronze reprocessada com sucesso — sigla_uf agora presente em Município, Meta por Município e Alunos.")
