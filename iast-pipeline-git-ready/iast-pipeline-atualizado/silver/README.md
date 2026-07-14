# Camada Silver — Indicador Criança Alfabetizada

Implementação da camada **Silver** (dados tratados e integrados) da pipeline
híbrida do Tech Challenge Fase 2, a partir dos dados já disponíveis na
camada Bronze.

## Por que PySpark

Stack ainda não definida pelo grupo → escolhido **PySpark** como base porque:
- roda local (`local[*]`) para desenvolvimento e testes sem custo de cluster;
- o mesmo código sobe sem alteração para Databricks, EMR (AWS) ou Dataproc (GCP);
- suporta nativamente **batch e streaming** (Structured Streaming) com a
  mesma API, o que atende ao requisito de pipeline híbrida do desafio;
- Parquet + particionamento (recomendação de FinOps do desafio) é suporte
  de primeira classe.

Se o grupo decidir por SQL puro em um Data Warehouse (BigQuery/Redshift), a
mesma lógica de regras (rename, tratamento de nulos, dedup, validações) se
traduz diretamente para `dbt models` — a estrutura de configuração em
`config.py` foi pensada para isso: virar `schema.yml` do dbt se for o caso.

## Estrutura

```
silver/
├── src/
│   ├── config.py              # caminhos, mapeamento de colunas, listas de referência
│   ├── main.py                 # orquestrador do pipeline batch completo
│   ├── jobs/
│   │   ├── base_job.py         # pipeline genérico: ler Bronze → padronizar → validar → gravar
│   │   ├── silver_dimensoes.py # UF, Município
│   │   ├── silver_metas.py     # Meta Brasil, Meta UF, Meta Município
│   │   ├── silver_alunos.py    # Dados de alunos (batch + callback de streaming)
│   │   └── silver_integration.py  # junção final de todas as bases
│   └── utils/
│       ├── standardize.py      # limpeza, tipos, chaves, nulos, dedup
│       ├── quality.py          # duplicidade, nulos, chaves, consistência
│       └── logging_utils.py
├── tests/
│   └── test_standardize_and_quality.py
└── requirements.txt
```

## O que cada etapa da Silver resolve (mapeado ao enunciado)

| Requisito do desafio | Onde está |
|---|---|
| Limpeza de dados | `standardize.trim_and_normalize_strings`, `snake_case_columns` |
| Tratamento de valores ausentes | `standardize.handle_missing_values` (chave nula → descarta linha; indicador nulo → mantém `null` + flag `_ausente`, nunca imputa 0/média para não distorcer o indicador) |
| Padronização de nomes e tipos | `standardize.apply_rename_map`, `cast_numeric_columns` |
| Normalização de chaves | `normalize_municipio_id` (zero-pad 7 dígitos IBGE), `normalize_uf_sigla` (2 letras maiúsculas) |
| Validação de consistência | `quality.check_consistencia_percentual`, `check_consistencia_ano`, `check_uf_valida` |
| Verificação de duplicidade | `quality.check_duplicidade` + `standardize.deduplicate` |
| Validação de chaves de relacionamento | `quality.check_chave_relacionamento` (ex.: todo `id_municipio` em Dados de Alunos existe na dimensão Município) |
| Integração das bases | `jobs/silver_integration.py` — junta Município + UF + as 3 granularidades de meta numa tabela única, pronta para a Gold |

## Como rodar (local, lendo Bronze do S3)

A Bronze já está num bucket S3 e o job roda na sua máquina local — isso
funciona via o conector `s3a://` do Hadoop, sem precisar de EMR/Glue.

### 1. Credenciais AWS
Configure do jeito que preferir (o código usa a `DefaultAWSCredentialsProviderChain`,
que tenta todos automaticamente, nesta ordem):
```bash
aws configure          # opção mais simples: salva em ~/.aws/credentials
# ou
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...     # só se usar credenciais temporárias/SSO
```

### 2. Instalar dependências
```bash
pip install -r requirements.txt
```

### 3. Ajustar `config.py`
Troque `s3a://seu-bucket/bronze` e `s3a://seu-bucket/silver` pelo bucket e
prefixo reais, e confira os nomes de coluna em `ENTITY_CONFIGS` contra o
schema real da sua Bronze (`df.printSchema()`).

### 4. Rodar
```bash
cd src
python main.py --local
```

Na primeira execução, o Spark baixa via Maven os jars `hadoop-aws` e
`aws-java-sdk-bundle` (configurado em `main.py::get_spark_session`) —
**precisa de internet nesse momento**, mesmo rodando "local". Depois disso
ficam em cache (`~/.ivy2`) e as próximas execuções não baixam de novo.

**Se sua máquina não tiver internet liberada para o Maven:** baixe os dois
`.jar` manualmente (versões `hadoop-aws:3.3.4` e
`aws-java-sdk-bundle:1.12.262` — ajuste para a versão de Hadoop que vem com
o seu `pyspark`, confira com `pyspark --version`) e aponte com
`--jars caminho/hadoop-aws.jar,caminho/aws-sdk-bundle.jar` em vez de
`spark.jars.packages`.

### Erros comuns
- `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem` → jars do
  passo acima não carregaram; confira a versão do Hadoop do seu pyspark.
- `403 Forbidden` / `Access Denied` no S3 → credencial sem permissão de
  leitura no bucket/prefixo, ou bucket em outra região (adicione
  `.config("spark.hadoop.fs.s3a.endpoint.region", "sua-regiao")` se
  necessário).
- Muito lento para listar muitos arquivos pequenos → normal do `s3a://`;
  se a Bronze tiver muitos arquivos pequenos, considere compactá-los.

Em produção (EMR/Glue/Databricks), esses jars já vêm embutidos no ambiente
— aí basta rodar `spark-submit main.py` sem a etapa de `spark.jars.packages`
manual.

## ⚠️ Ação necessária antes de rodar

Os nomes de coluna em `config.py` (`ENTITY_CONFIGS[...].rename_map`) são um
ponto de partida baseado nas entidades descritas no desafio. **Rode
`df.printSchema()` nos seus arquivos Bronze reais e ajuste o `rename_map`**
para bater com os nomes de coluna que a Base dos Dados realmente entrega
(nomes podem variar por tabela/ano exportado).

## Streaming

`jobs/silver_alunos.py::process_alunos_microbatch` reaproveita exatamente a
mesma função de padronização (`standardize_entity`) usada no batch — só
muda o trigger. Exemplo de job separado para plugar isso:

```python
(
    spark.readStream.format("...")  # Kafka/Kinesis/Event Hubs/autoloader
    .load()
    .writeStream
    .foreachBatch(process_alunos_microbatch)
    .trigger(processingTime="1 minute")
    .start()
)
```

## Testes

```bash
pytest tests/ -v
```

## Monitoramento

Cada job grava um relatório JSON de qualidade em
`data/silver/_quality_reports/`, com status `OK`/`ALERTA` por regra. Isso
serve de insumo direto para o item de observabilidade do desafio — em
nuvem, esse mesmo JSON pode ser enviado para CloudWatch/Log Analytics/Cloud
Logging e disparar alertas.
