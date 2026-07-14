"""
Simulador de eventos de streaming — novas avaliações/medições de alunos
chegando quase em tempo real (conforme o enunciado do desafio).

Publica eventos no formato da tabela `dados_alunos` no Kinesis Data Stream,
para serem consumidos pelo job `run_silver_streaming.py`, que reaproveita a
mesma função `process_alunos_microbatch` usada no batch.

Uso:
    python kinesis_producer_simulator.py --stream-name iast-alunos-events \
        --region us-east-1 --interval 2 --n-events 50
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3

MUNICIPIOS = [
    ("2304400", "CE", "Fortaleza"),
    ("2307650", "CE", "Quixadá"),
    ("3550308", "SP", "São Paulo"),
    ("2927408", "BA", "Salvador"),
]


def gerar_evento_aluno(ano: int) -> dict:
    id_municipio, sigla_uf, nome_municipio = random.choice(MUNICIPIOS)
    return {
        "ano": ano,
        "id_municipio": id_municipio,
        "id_municipio_nome": nome_municipio,
        "sigla_uf": sigla_uf,
        "id_escola": str(random.randint(1000, 9999)),
        "id_aluno": str(uuid.uuid4())[:8],
        "caderno": random.randint(1, 20),
        "serie": "2° ano do Ensino Fundamental",
        "rede": random.choice(["Municipal", "Estadual", "Pública"]),
        "presenca": "Presente",
        "preenchimento_caderno": "Prova preenchida",
        "alfabetizado": random.choice(["Sim", "Não"]),
        "proficiencia": round(random.uniform(600, 900), 2),
        "peso_aluno": round(random.uniform(0.8, 1.3), 2),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream-name", default="iast-alunos-events")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--n-events", type=int, default=20)
    parser.add_argument("--ano", type=int, default=datetime.now().year)
    args = parser.parse_args()

    client = boto3.client("kinesis", region_name=args.region)

    for i in range(args.n_events):
        evento = gerar_evento_aluno(args.ano)
        client.put_record(
            StreamName=args.stream_name,
            Data=json.dumps(evento).encode("utf-8"),
            PartitionKey=evento["id_municipio"],
        )
        print(f"[{datetime.now(timezone.utc).isoformat()}] Evento {i+1}/{args.n_events}: {evento['id_municipio_nome']}/{evento['sigla_uf']} — {evento['id_aluno']}")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
