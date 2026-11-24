# Configuração dos logs de servidor, LB e clientes

import logging as log
from pathlib import Path


directory_logs = Path("./logs")
directory_logs.mkdir(parents=True, exist_ok=True)


def configure_logs(identifier):
    log_conf = log.getLogger(identifier)
    log_conf.setLevel(log.INFO)
    log_conf.propagate = False
    if not log_conf.handlers:
        handler = log.FileHandler(
            directory_logs / f"{identifier}.log",
            encoding="utf-8"
        )
        formatter = log.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        log_conf.addHandler(handler)
    log_conf.info("Arquivo de logs -> %s <- iniciado!", identifier)
    return log_conf


# Preparar os logs dos componentes e do menu ao carregar o módulo

server_logs = configure_logs("Server")
load_balanced_logs = configure_logs("Load_balance")
clients_logs = configure_logs("Clients")

managenment_logs = configure_logs("managenment")
