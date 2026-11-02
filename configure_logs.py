import logging as log
from pathlib import Path

directory_logs = Path("./logs")

directory_logs.mkdir(parents=True, exist_ok=True)



def configure_logs(identifier):
    log_conf = log.getLogger(identifier)
    # níveis para settar caso precise: DEBUG/INFO/WARNING/ERROR/CRITICAL
    log_conf.setLevel(log.INFO)

    log_conf.propagate = False ##não vamos precisar subir as informações para outro lugar né?


    if not log_conf.handlers:
        handler = log.FileHandler(directory_logs /f"{identifier}.log",   encoding="utf-8")

        formatter = log.Formatter("%(asctime)s |%(levelname)s | %(message)s",datefmt="%Y-%m -%d %H:%M:%S")

        handler.setFormatter(formatter)
        log_conf.addHandler(handler)

    log_conf.info(f"Arquivo de logs -> {identifier} <- iniciado!")

    return log_conf


##criando os logs aqui pra já ficar certo
server_logs = configure_logs("Server")
load_balanced_logs = configure_logs("Load_balance")
clients_logs = configure_logs("Clients")