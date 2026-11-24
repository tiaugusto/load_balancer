"""Gerar M clientes conectados somente ao LB durante K segundos."""

import argparse
from concurrent.futures import ThreadPoolExecutor, wait
from io import DEFAULT_BUFFER_SIZE
import ipaddress
import json
import math
import socket
import threading
import time

from management.configure_logs import clients_logs


def run_load(address, clients, duration, duration_step=0, interval=0.1, timeout=1, stop=None):
    """Executar a carga concorrente e reunir os resultados de cada cliente."""
    if type(clients) is not int or clients < 1:
        raise ValueError("M deve ser um inteiro positivo")
    if not all(math.isfinite(v) and v > 0 for v in [duration, interval, timeout]):
        raise ValueError("Duração, intervalo e timeout devem ser positivos e finitos")
    if not math.isfinite(duration_step) or duration_step < 0:
        raise ValueError("Variação de duração deve ser não negativa e finita")
    if not math.isfinite(duration + (clients - 1) * duration_step):
        raise ValueError("Duração final deve ser finita")
    start = threading.Event()
    stop = stop if stop is not None else threading.Event()
    active_connections = set()
    connections_lock = threading.Lock()

    def client(index):
        start.wait()
        started = time.monotonic()
        seconds = duration + index * duration_step
        deadline = started + seconds
        sent = received = 0
        error = None
        connection = None
        try:
            connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            with connections_lock:
                active_connections.add(connection)
            if stop.is_set():
                raise ConnectionAbortedError("Carga interrompida")
            connection.settimeout(min(timeout, seconds))
            connection.connect(address)
            clients_logs.info("Cliente %s conectado ao LB; duração=%.2fs", index, seconds)
            payload = ("cliente=%s\n" % index).encode("utf-8")
            while not stop.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                connection.settimeout(min(timeout, remaining))
                connection.sendall(payload)
                sent += len(payload)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                connection.settimeout(min(timeout, remaining))
                try:
                    data = connection.recv(DEFAULT_BUFFER_SIZE)
                except socket.timeout:
                    continue
                if not data:
                    raise ConnectionError("LB encerrou a conexão antes da duração prevista")
                received += len(data)
                stop.wait(min(interval, max(0, deadline - time.monotonic())))
            if stop.is_set():
                error = "Carga interrompida"
            elif not received:
                error = "Nenhuma resposta recebida do LB durante o teste"
        except OSError as failure:
            error = "Carga interrompida" if stop.is_set() else str(failure)
        finally:
            if connection is not None:
                with connections_lock:
                    active_connections.discard(connection)
                connection.close()
        clients_logs.info("Cliente %s terminou; enviados=%s recebidos=%s erro=%s",
                          index, sent, received, error)
        return {
            "client": index,
            "requested_seconds": seconds,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "bytes_sent": sent,
            "bytes_received": received,
            "error": error
        }

    def disconnect():
        with connections_lock:
            connections = list(active_connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()

    with ThreadPoolExecutor(max_workers=clients) as executor:
        try:
            futures = [executor.submit(client, index) for index in range(clients)]
            start.set()
            pending = set(futures)
            while pending and not stop.is_set():
                _, pending = wait(pending, timeout=0.1)
            if stop.is_set():
                disconnect()
            results = [future.result() for future in futures]
        except BaseException:
            stop.set()
            start.set()
            disconnect()
            raise
    return {
        "clients": clients,
        "successful": sum(result["error"] is None for result in results),
        "failed": sum(result["error"] is not None for result in results),
        "bytes_sent": sum(result["bytes_sent"] for result in results),
        "bytes_received": sum(result["bytes_received"] for result in results),
        "results": results
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="IPv4 do LB")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--clients", "-M", type=int, required=True)
    parser.add_argument("--duration", "-K", type=float, required=True)
    parser.add_argument("--duration-step", type=float, default=0)
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--timeout", type=float, default=1)
    args = parser.parse_args()
    try:
        address = (str(ipaddress.IPv4Address(args.host)), args.port)
        if not 1 <= args.port <= 65535:
            raise ValueError("Porta deve estar entre 1 e 65535")
        report = run_load(address, args.clients, args.duration, args.duration_step,
                          args.interval, args.timeout)
    except ValueError as error:
        parser.error(str(error))
    except KeyboardInterrupt:
        return 130
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if report["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
