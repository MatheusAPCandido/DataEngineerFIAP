"""
Monitoramento — publica no CloudWatch as métricas derivadas dos relatórios
de qualidade que a Silver já grava (`base_job.save_quality_report`), sem
precisar mudar nada na lógica de qualidade existente.

Uso:
    # depois de rodar o pipeline Silver, aponte para a pasta de relatórios:
    python publish_quality_metrics.py --reports-dir data/silver/_quality_reports --region us-east-1

    # provisiona os alarmes uma única vez:
    python setup_cloudwatch_alarms.py --region us-east-1 --email time-dados@exemplo.com
"""

import argparse
import glob
import json
import os

import boto3

NAMESPACE = "IAST/AlfabetizacaoPipeline"


def publish_quality_metrics(reports_dir: str, region: str) -> None:
    """Lê cada relatório JSON salvo por `base_job.save_quality_report` e
    publica métricas customizadas: 1 alarme potencial por entidade com
    status ALERTA, e a contagem de checks OK/ALERTA."""
    cw = boto3.client("cloudwatch", region_name=region)

    for path in glob.glob(os.path.join(reports_dir, "*.json")):
        with open(path, encoding="utf-8") as f:
            report = json.load(f)

        entidade = report.get("entidade", os.path.basename(path))
        status_geral = report.get("status_geral", "DESCONHECIDO")
        checks = report.get("checks", [])
        n_alertas = sum(1 for c in checks if c.get("status") == "ALERTA")

        cw.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[
                {
                    "MetricName": "QualityCheckAlerts",
                    "Dimensions": [{"Name": "Entidade", "Value": entidade}],
                    "Value": n_alertas,
                    "Unit": "Count",
                },
                {
                    "MetricName": "PipelineRunStatus",
                    "Dimensions": [{"Name": "Entidade", "Value": entidade}],
                    "Value": 0 if status_geral == "OK" else 1,
                    "Unit": "Count",
                },
            ],
        )
        print(f"[MONITORING] '{entidade}': status={status_geral}, alertas={n_alertas} — métrica publicada")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reports-dir", default="data/silver/_quality_reports")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()
    publish_quality_metrics(args.reports_dir, args.region)
