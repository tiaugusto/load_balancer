"""Demonstrar distribuição, contadores e recuperação com processos reais."""

import argparse
import json
import os
import signal
import socket
import threading
import time

from clients import run_load
from load_balance import LoadBalancer
from management.monitoramento import Monitor
from management.politcs import LeastConnections, RoundRobin
from servers.server_class import Server
from servers.Table import ServerTable


def free_addresses(number):
    reservations = [socket.socket() for _ in range(number)]
    try:
        for connection in reservations:
            connection.bind(("127.0.0.1", 0))
        return [connection.getsockname() for connection in reservations]
    finally:
        for connection in reservations:
            connection.close()


def wait_until(predicate, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        threading.Event().wait(0.01)
    raise AssertionError("Estado esperado não foi observado")


def demonstrate(policy_name, number=3, restart_delay=0.4):
    table = ServerTable(
        filename=None,
        servers=[
            Server(i, *address, _enable='YES' if i < number else 'NO')
            for (i, address) in enumerate(free_addresses(number + 1))
        ],
        policy_class=RoundRobin if policy_name == 'round-robin' else LeastConnections
    )
    monitor = Monitor(table, interval=0.05, timeout=0.2, restart_delay=restart_delay)
    lb = thread = None
    clients, scenarios = [], []

    def connect():
        client = socket.create_connection(lb.address, timeout=5)
        replies = client.makefile("rb")
        clients.append((client, replies))
        client.sendall(b"pedido\n")
        reply = replies.readline()
        assert reply.startswith(b"OK server=") and reply.endswith(b"\n"), reply
        return client, replies, int(reply[len(b"OK server="):-1])

    def close(client, replies):
        try:
            client.shutdown(socket.SHUT_WR)
            replies.read()
        except OSError:
            pass
        finally:
            replies.close()
            client.close()
            clients.remove((client, replies))

    try:
        monitor.start()
        assert set(monitor.get_processes()) == set(range(number))
        lb = LoadBalancer(("127.0.0.1", 0), table.select_server, table.release_server)
        thread = threading.Thread(target=lb.serve_forever)
        thread.start()
        connections = [connect() for _ in range(number)]
        assert [c[2] for c in connections] == list(range(number))
        assert table.snapshot()["num_total_connections"] == number
        scenarios.append("distribuicao_e_exclusao_do_cadastro_desabilitado")

        short, replies, short_id = connections.pop()
        close(short, replies)
        wait_until(lambda: table.get_servers()[short_id].num_connections == 0)
        incoming = connect()
        assert incoming[2] == (0 if policy_name == "round-robin" else short_id)
        for client, replies in list(clients):
            close(client, replies)
        wait_until(lambda: table.snapshot()["num_total_connections"] == 0)
        scenarios.append("escolha_apos_conexao_curta")

        report = run_load(lb.address, 6, 0.25, duration_step=0.04, interval=0.02)
        assert report["successful"] == 6
        wait_until(lambda: table.snapshot()["num_total_connections"] == 0)
        scenarios.append("carga_M_K_com_duracoes_variadas")

        first, replies, victim = connect()
        other, other_replies, other_id = connect()
        assert victim != other_id
        old_pid = monitor.get_processes()[victim]
        os.kill(old_pid, signal.SIGKILL)
        wait_until(lambda: table.get_servers()[victim].status_server == "DOWN")
        other.sendall(b"continua\n")
        assert other_replies.readline() == ("OK server=%s\n" % other_id).encode()
        available = connect()
        assert available[2] != victim
        close(available[0], available[1])
        wait_until(lambda: table.get_servers()[victim].status_server == "UP")
        assert monitor.get_processes()[victim] != old_pid
        close(first, replies)
        close(other, other_replies)
        wait_until(lambda: table.snapshot()["num_total_connections"] == 0)
        scenarios.append("falha_e_recuperacao_preservando_outro_cliente")

        for pid in monitor.get_processes().values():
            os.kill(pid, signal.SIGKILL)
        wait_until(lambda: all(s.status_server == "DOWN" for s in table.get_servers()))
        with socket.create_connection(lb.address, timeout=5) as rejected:
            assert rejected.recv(1) == b""
        wait_until(lambda: all(s.status_server == "UP" for s in table.get_servers() if (s._enable == "YES")))
        incoming = connect()
        close(incoming[0], incoming[1])
        scenarios.append("indisponibilidade_total_e_retorno")
    finally:
        for client, replies in list(clients):
            close(client, replies)
        if lb is not None:
            lb.shutdown()
        if thread is not None:
            thread.join(5)
            assert not thread.is_alive()
        monitor.stop()
    assert table.snapshot()["num_total_connections"] == 0
    assert not monitor.get_processes()
    return {"policy": policy_name, "servers": number, "success": True, "scenarios": scenarios,
            "final_connections": 0, "child_processes": 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", choices=["round-robin", "least-connections"], default="round-robin")
    parser.add_argument("--servers", type=int, default=3)
    parser.add_argument("--restart-delay", type=float, default=0.4)
    args = parser.parse_args()
    if not 2 <= args.servers <= 8 or not 0.2 <= args.restart_delay <= 5:
        parser.error("Use de 2 a 8 servidores e espera de 0.2 a 5 segundos na demonstração")
    print(json.dumps(demonstrate(args.policy, args.servers, args.restart_delay), indent=2))


if __name__ == "__main__":
    main()
