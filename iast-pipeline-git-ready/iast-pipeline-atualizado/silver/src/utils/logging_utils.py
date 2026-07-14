import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Logger padrão do pipeline. Em produção, plugar aqui o handler do
    serviço de observabilidade da nuvem escolhida (CloudWatch, Log Analytics,
    Cloud Logging) para alimentar o monitoramento de falhas/latência."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
