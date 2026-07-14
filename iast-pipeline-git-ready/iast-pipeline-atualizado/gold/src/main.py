"""
Orquestrador da camada Gold.

Uso:
    spark-submit main.py
    python main.py --local     # dev/teste local
"""

import argparse

from pyspark.sql import SparkSession

from jobs.build_gold import run_gold_pipeline


def get_spark_session(local: bool) -> SparkSession:
    builder = SparkSession.builder.appName("gold-indicador-alfabetizacao")
    if local:
        builder = builder.master("local[*]").config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ).config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        ).config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
    return builder.getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()

    spark = get_spark_session(local=args.local)
    try:
        run_gold_pipeline(spark)
        print("=== Pipeline Gold concluído com sucesso ===")
    finally:
        spark.stop()
