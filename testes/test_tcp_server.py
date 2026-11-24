# Testes do servidor TCP, do processo e do fluxo completo com o LB

import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from clients import run_load
from load_balance import LoadBalancer
from management.monitoramento import probe_server
from management.politcs import LeastConnections, RoundRobin
from servers.Server import TCPServer
from servers.server_class import Server
from servers.Table import ServerTable


class TCPServerTests(unittest.TestCase):

    # Preparar servidor, LB e clientes

    def start(self, service):
        errors = []

        def serve():
            try:
                service.serve_forever()
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=serve)
        thread.start()

        def close():
            service.shutdown()
            thread.join(5)
            self.assertFalse(thread.is_alive(), "Serviço não encerrou")
            self.assertEqual(errors, [])

        self.addCleanup(close)
        return service

    def server(self, server_id=7, **options):
        return self.start(TCPServer(("127.0.0.1", 0), server_id, **options))

    def client(self, service):
        connection = socket.create_connection(service.address, timeout=5)
        self.addCleanup(connection.close)
        return connection

    # Conferir respostas e conexões simultâneas

    def test_acknowledges_binary_data_and_keeps_connection_open(self):
        server = self.server()
        client = self.client(server)
        with client.makefile("rb") as replies:
            for payload in [b"pedido\x00\xff", b"outro pedido"]:
                client.sendall(payload)
                self.assertEqual(replies.readline(), b"OK server=7\n")
            client.shutdown(socket.SHUT_WR)
            self.assertEqual(replies.read(), b"")

    def test_large_stream_is_acknowledged_without_assuming_recv_boundaries(self):
        server = self.server()
        client = self.client(server)
        client.sendall(bytes(range(256)) * 1024)
        client.shutdown(socket.SHUT_WR)
        with client.makefile("rb") as replies:
            confirmations = replies.read().splitlines()
        self.assertGreater(len(confirmations), 0)
        self.assertTrue(all(reply == b"OK server=7" for reply in confirmations))

    def test_idle_connection_does_not_block_another_client(self):
        server = self.server()
        idle = self.client(server)
        active = self.client(server)
        with active.makefile("rb") as replies:
            active.sendall(b"pedido")
            self.assertEqual(replies.readline(), b"OK server=7\n")
        idle.close()

    def test_multiple_clients_receive_before_any_of_them_disconnects(self):
        server = self.server()
        barrier = threading.Barrier(8)

        def exchange(_):
            with socket.create_connection(server.address, timeout=5) as client:
                with client.makefile("rb") as replies:
                    client.sendall(b"pedido")
                    reply = replies.readline()
                    barrier.wait(timeout=5)
                    return reply

        with ThreadPoolExecutor(max_workers=8) as executor:
            replies = list(executor.map(exchange, range(8)))
        self.assertEqual(replies, [b"OK server=7\n"] * 8)

    def test_shutdown_interrupts_idle_reads_and_can_be_repeated(self):
        server = self.server()
        client = self.client(server)
        with client.makefile("rb") as replies:
            client.sendall(b"pedido")
            self.assertEqual(replies.readline(), b"OK server=7\n")
            server.shutdown()
            self.assertEqual(replies.read(), b"")
        server.shutdown()

    def test_restarts_on_the_same_ip_and_port(self):
        first = self.server(server_id=7)
        first.shutdown()
        second = self.start(TCPServer(first.address, server_id=8))
        client = self.client(second)
        with client.makefile("rb") as replies:
            client.sendall(b"pedido")
            self.assertEqual(replies.readline(), b"OK server=8\n")


    def test_worker_start_failure_preserves_existing_and_future_clients(self):
        server = self.server()
        first = self.client(server)
        original_start = threading.Thread.start
        failed = threading.Event()

        def start(worker):
            if worker._target == server._handle_connection and not failed.is_set():
                failed.set()
                raise RuntimeError("Falha temporária ao criar thread")
            return original_start(worker)

        with first.makefile("rb") as replies:
            first.sendall(b"ok")
            self.assertEqual(replies.readline(), b"OK server=7\n")
            with patch.object(threading.Thread, "start", start):
                rejected = self.client(server)
                self.assertEqual(rejected.recv(1), b"")
                self.assertTrue(failed.is_set())
                first.sendall(b"continua")
                self.assertEqual(replies.readline(), b"OK server=7\n")
                third = self.client(server)
                with third.makefile("rb") as incoming:
                    third.sendall(b"novo")
                    self.assertEqual(incoming.readline(), b"OK server=7\n")

    def test_monitor_still_responds_with_128_clients_through_lb(self):
        server = self.server()
        table = ServerTable(
            filename=None,
            servers=[Server(7, *server.address, status_server='UP', _enable='YES')]
        )
        balancer = self.start(LoadBalancer(("127.0.0.1", 0),
                                          table.select_server, table.release_server))
        clients = []
        for _ in range(128):
            client = self.client(balancer)
            replies = client.makefile("rb")
            self.addCleanup(replies.close)
            client.sendall(b"pedido")
            self.assertEqual(replies.readline(), b"OK server=7\n")
            clients.append((client, replies))
        self.assertTrue(probe_server(table.get_servers()[0], timeout=0.5))
        self.assertEqual(table.snapshot()["servers"][0]["status_server"], "UP")
        self.assertEqual(table.snapshot()["num_total_connections"], 128)
        client, replies = clients[0]
        client.sendall(b"continua")
        self.assertEqual(replies.readline(), b"OK server=7\n")
        balancer.shutdown()
        self.assertEqual(table.snapshot()["num_total_connections"], 0)

    def test_real_servers_receive_concurrent_connections_through_lb(self):
        for policy in [RoundRobin, LeastConnections]:
            with self.subTest(policy=policy.__name__):
                first = self.server(server_id=0)
                second = self.server(server_id=1)
                down = self.server(server_id=2)
                table = ServerTable(
                    filename=None,
                    servers=[
                        Server(0, *first.address, status_server='UP', _enable='YES'),
                        Server(1, *second.address, status_server='UP', _enable='YES'),
                        Server(2, *down.address, status_server='DOWN', _enable='YES')
                    ],
                    policy_class=policy
                )
                balancer = self.start(LoadBalancer(("127.0.0.1", 0),
                                                   table.select_server, table.release_server))
                arrived = threading.Barrier(9)
                finish = threading.Event()

                def exchange(_):
                    with socket.create_connection(balancer.address, timeout=5) as client:
                        with client.makefile("rb") as replies:
                            client.sendall(b"pedido")
                            reply = replies.readline()
                            arrived.wait(timeout=5)
                            if not finish.wait(5):
                                raise AssertionError("Teste não autorizou encerramento")
                            client.shutdown(socket.SHUT_WR)
                            self.assertEqual(replies.read(), b"")
                            return reply

                with ThreadPoolExecutor(max_workers=8) as executor:
                    pending = [executor.submit(exchange, index) for index in range(8)]
                    try:
                        arrived.wait(timeout=5)
                        state = table.snapshot()
                        self.assertEqual(state["num_total_connections"], 8)
                        self.assertEqual([s["num_connections"] for s in state["servers"]],
                                         [4, 4, 0])
                    finally:
                        finish.set()
                    replies = [result.result(timeout=5) for result in pending]
                self.assertEqual(Counter(replies), {b"OK server=0\n": 4,
                                                   b"OK server=1\n": 4})
                balancer.shutdown()
                self.assertEqual(table.snapshot()["num_total_connections"], 0)

    def test_load_generator_works_with_real_server_and_lb(self):
        for policy in [RoundRobin, LeastConnections]:
            with self.subTest(policy=policy.__name__):
                server = self.server()
                table = ServerTable(
                    filename=None,
                    servers=[Server(7, *server.address, status_server='UP', _enable='YES')],
                    policy_class=policy
                )
                balancer = self.start(LoadBalancer(("127.0.0.1", 0),
                                                   table.select_server, table.release_server))
                report = run_load(balancer.address, clients=4, duration=0.2,
                                  duration_step=0.05, interval=0.01)
                self.assertEqual(report["successful"], 4)
                self.assertEqual(report["failed"], 0)
                self.assertGreater(report["bytes_received"], 0)
                balancer.shutdown()
                self.assertEqual(table.snapshot()["num_total_connections"], 0)

    def test_down_status_blocks_new_connections_without_cutting_existing_ones(self):
        for policy in [RoundRobin, LeastConnections]:
            with self.subTest(policy=policy.__name__):
                first = self.server(server_id=0)
                second = self.server(server_id=1)
                table = ServerTable(
                    filename=None,
                    servers=[
                        Server(0, *first.address, status_server='UP', _enable='YES'),
                        Server(1, *second.address, status_server='UP', _enable='YES')
                    ],
                    policy_class=policy
                )
                balancer = self.start(LoadBalancer(("127.0.0.1", 0),
                                                   table.select_server, table.release_server))
                existing = self.client(balancer)
                with existing.makefile("rb") as replies:
                    existing.sendall(b"primeiro pedido")
                    self.assertEqual(replies.readline(), b"OK server=0\n")
                    table.update_status_server(0, "DOWN")
                    incoming = self.client(balancer)
                    with incoming.makefile("rb") as next_replies:
                        incoming.sendall(b"novo pedido")
                        self.assertEqual(next_replies.readline(), b"OK server=1\n")
                    existing.sendall(b"conexao continua")
                    self.assertEqual(replies.readline(), b"OK server=0\n")
                    self.assertEqual(table.snapshot()["num_total_connections"], 2)
                    table.update_status_server(0, "DOWN")
                balancer.shutdown()
                self.assertEqual(table.snapshot()["num_total_connections"], 0)


class TCPServerCLITests(unittest.TestCase):

    # Executar o script como processo separado

    def launch(self, port, directory):
        root = Path(__file__).resolve().parents[1]
        environment = dict(os.environ, PYTHONPATH=str(root))
        process = subprocess.Popen([sys.executable, "-m", "servers.Server", "--port", str(port), "--id", "42"],
                                   cwd=directory, text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=environment)
        ready = threading.Event()
        output = []

        def read_output():
            for line in process.stderr:
                output.append(line)
                if "ouvindo em" in line:
                    ready.set()

        reader = threading.Thread(target=read_output)
        reader.start()

        def close():
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            reader.join(5)
            process.stdout.close()
            process.stderr.close()

        self.addCleanup(close)
        self.assertTrue(ready.wait(5), "".join(output))
        return process

    def test_cli_stops_with_active_connection_and_restarts(self):
        with tempfile.TemporaryDirectory() as directory:
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            for _ in range(2):
                process = self.launch(port, directory)
                with socket.create_connection(("127.0.0.1", port), timeout=5) as client:
                    with client.makefile("rb") as replies:
                        client.sendall(b"pedido")
                        self.assertEqual(replies.readline(), b"OK server=42\n")
                        process.send_signal(signal.SIGINT)
                        self.assertEqual(process.wait(timeout=5), 0)
                        self.assertEqual(replies.read(), b"")

    def test_cli_rejects_invalid_configuration_without_traceback(self):
        root = Path(__file__).resolve().parents[1]
        for arguments in [["--port", "0"], ["--port", "70000"],
                          ["--port", "9001", "--host", "invalid"],
                          ["--port", "9001", "--id", "-1"]]:
            with self.subTest(arguments=arguments):
                result = subprocess.run([sys.executable, "-m", "servers.Server"] + arguments,
                                        cwd=root, capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 2)
                self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
