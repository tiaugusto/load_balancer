"""Iniciar servidores, monitor e LB usando a mesma tabela em memória."""

import argparse
import ipaddress
import logging
from pathlib import Path
import threading
import signal

from management.configure_logs import load_balanced_logs
from load_balance import LoadBalancer
from management.monitoramento import Monitor, monitor_logs
from management.politcs import LeastConnections, RoundRobin
from servers.Table import ServerTable


def run(args, should_stop):
    policy_class = RoundRobin if args.policy == "round-robin" else LeastConnections
    table = ServerTable(Path(args.servers_file).resolve(), policy_class=policy_class)
    monitor = Monitor(table, interval=args.check_interval, timeout=args.probe_timeout,
                      restart_delay=args.restart_delay, max_restarts=args.max_restarts,
                      should_stop=should_stop)
    balancer = None
    listener_thread = None
    errors = []
    try:
        monitor.start()
        if not should_stop():
            balancer = LoadBalancer((args.host, args.port), table.select_server,
                                    table.release_server, should_stop=should_stop)
            load_balanced_logs.info("Política selecionada: %s", args.policy)
            if args.menu:
                from menu import run_menu

                def serve():
                    try:
                        balancer.serve_forever()
                    except Exception as error:
                        errors.append(error)

                listener_thread = threading.Thread(target=serve)
                listener_thread.start()
                run_menu(table, monitor, lambda: should_stop() or bool(errors))
                if errors:
                    raise errors[0]
            else:
                balancer.serve_forever()
    finally:
        try:
            if balancer is not None:
                balancer.shutdown()
            if listener_thread is not None and listener_thread.ident is not None:
                listener_thread.join()
        finally:
            monitor.stop()
            load_balanced_logs.info("Sistema encerrado; total de conexões=%s",
                                    table.snapshot()["num_total_connections"])


def main(default_menu=False):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--servers-file",
                        default=str(Path(__file__).parent / "servers/tabela_servidores.json"))
    parser.add_argument("--menu", action="store_true", default=default_menu)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--policy", choices=["round-robin", "least-connections"], default="round-robin")
    parser.add_argument("--check-interval", type=float, default=2)
    parser.add_argument("--probe-timeout", type=float, default=0.5)
    parser.add_argument("--restart-delay", type=float, default=15)
    parser.add_argument("--max-restarts", type=int, default=3)
    args = parser.parse_args()
    stopping = False

    def request_stop(signum, frame):
        nonlocal stopping
        # O laço normal fará a limpeza; não interromper a criação dos filhos.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    for logger in [load_balanced_logs, monitor_logs]:
        logger.addHandler(logging.StreamHandler())
    try:
        args.host = str(ipaddress.IPv4Address(args.host))
        if not 1 <= args.port <= 65535:
            raise ValueError("Porta deve estar entre 1 e 65535")
        run(args, lambda: stopping)
    except (OSError, ValueError, RuntimeError) as error:
        monitor_logs.error("Não foi possível executar o sistema: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
