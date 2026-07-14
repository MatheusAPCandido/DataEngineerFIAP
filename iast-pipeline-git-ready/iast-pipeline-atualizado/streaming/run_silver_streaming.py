"""
Job de streaming — consome o Kinesis Data Stream com eventos de novas
avaliações de alunos e grava na Silver, reaproveitando exatamente a mesma
função de padronização/qualidade usada no batch
(`silver/src/jobs/silver_alunos.py::process_alunos_microbatch`), conforme
já estava planejado no README da Silver.

Requer o conector spark-sql-kinesis (Qubole) ou, alternativamente, o
Kinesis Client Library via um source customizado — o exemplo abaixo usa o
formato "kinesis" disponível nos runtimes gerenciados (EMR/Databricks); se
for rodar fora deles, troque o `.format("kinesis")` pelo conector que seu
ambiente suportar (ex.: "aws-kinesis" da Qubole, incluído via --packages).

Uso (EMR/Databricks, com o conector Kinesis já disponível no cluster):
    spark-submit run_silver_streaming.py --stream-name iast-alunos-events --region us-east-1
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "silver", "src"))

from pyspark.sql import SparkSession

from jobs.silver_alunos import process_alunos_microbatch  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream-name", default="iast-alunos-events")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--trigger-seconds", type=int, default=60)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("silver-alunos-streaming").getOrCreate()

    # Schema do evento — precisa bater com o Bronze/dados_alunos para que
    # standardize_entity() funcione sem alteração.
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

    schema = StructType([
        StructField("ano", IntegerType()),
        StructField("id_municipio", StringType()),
        StructField("id_municipio_nome", StringType()),
        StructField("sigla_uf", StringType()),
        StructField("id_escola", StringType()),
        StructField("id_aluno", StringType()),
        StructField("caderno", IntegerType()),
        StructField("serie", StringType()),
        StructField("rede", StringType()),
        StructField("presenca", StringType()),
        StructField("preenchimento_caderno", StringType()),
        StructField("alfabetizado", StringType()),
        StructField("proficiencia", DoubleType()),
        StructField("peso_aluno", DoubleType()),
    ])

    from pyspark.sql import functions as F

    df_raw = (
        spark.readStream.format("kinesis")
        .option("streamName", args.stream_name)
        .option("region", args.region)
        .option("initialPosition", "LATEST")
        .load()
    )

    df_parsed = df_raw.select(
        F.from_json(F.col("data").cast("string"), schema).alias("evento")
    ).select("evento.*")

    query = (
        df_parsed.writeStream.foreachBatch(process_alunos_microbatch)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .option("checkpointLocation", "s3a://tech-fiap-ai/checkpoints/silver_alunos_streaming/")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
