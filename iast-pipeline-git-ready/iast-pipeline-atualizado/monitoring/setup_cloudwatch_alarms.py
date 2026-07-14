"""
Provisiona os alarmes CloudWatch para a pipeline (falhas de qualidade,
falta de execução, volume anômalo). Roda uma única vez (ou via IaC).
"""

import argparse

import boto3

NAMESPACE = "IAST/AlfabetizacaoPipeline"


def setup_alarms(region: str, email: str) -> str:
    sns = boto3.client("sns", region_name=region)
    cw = boto3.client("cloudwatch", region_name=region)

    topico = sns.create_topic(Name="iast-pipeline-alertas")
    topic_arn = topico["TopicArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    print(f"[MONITORING] Tópico SNS criado: {topic_arn} (confirme a inscrição no e-mail {email})")

    alarmes = [
        {
            "AlarmName": "IAST-Qualidade-Com-Alerta",
            "MetricName": "PipelineRunStatus",
            "Statistic": "Maximum",
            "Threshold": 0,
            "ComparisonOperator": "GreaterThanThreshold",
            "EvaluationPeriods": 1,
            "Period": 3600,
        },
        {
            "AlarmName": "IAST-Muitos-Alertas-Qualidade",
            "MetricName": "QualityCheckAlerts",
            "Statistic": "Sum",
            "Threshold": 3,
            "ComparisonOperator": "GreaterThanThreshold",
            "EvaluationPeriods": 1,
            "Period": 3600,
        },
    ]

    for alarme in alarmes:
        cw.put_metric_alarm(
            AlarmName=alarme["AlarmName"],
            Namespace=NAMESPACE,
            MetricName=alarme["MetricName"],
            Statistic=alarme["Statistic"],
            Period=alarme["Period"],
            EvaluationPeriods=alarme["EvaluationPeriods"],
            Threshold=alarme["Threshold"],
            ComparisonOperator=alarme["ComparisonOperator"],
            AlarmActions=[topic_arn],
            TreatMissingData="notBreaching",
        )
        print(f"[MONITORING] Alarme criado: {alarme['AlarmName']}")

    return topic_arn


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--email", required=True)
    args = parser.parse_args()
    setup_alarms(args.region, args.email)
