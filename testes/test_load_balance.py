# Testes do encaminhamento TCP e da integração com a tabela

import socket
import struct
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from types import SimpleNamespace
from unittest.mock import patch

from load_balance import LoadBalancer, close_connection
from management.politcs import LeastConnections, RoundRobin
from servers.server_class import Server
from servers.Table import ServerTable


# Receber dados e preparar o servidor de teste

def receive_all(connection):
    chunks = []
    while True:
        data = connection.recv(65536)
        if not data:
            return b"".join(chunks)
        chunks.append(data)


def receive_exact(connection, length):
    data = bytearray()
    while len(data) < length:
        chunk = connection.recv(length - len(data))
        if not chunk:
            raise AssertionError("Conexão terminou antes de receber todos os dados")
        data.extend(chunk)
    return bytes(data)


def echo(connection):
    while True:
        data = connection.recv(65536)
        if not data:
            return
        connection.sendall(data)


class TCPBackend:

    def __init__(self, handler):
        self.handler = handler
        self.stopped = threading.Event()
        self.lock = threading.Lock()
        self.connections = []
        self.workers = []
        self.errors = []
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.address = self.listener.getsockname()
        self.thread = threading.Thread(target=self.serve)
        self.thread.start()

    def serve(self):
        while not self.stopped.is_set():
            try:
                connection, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if self.stopped.is_set():
                    return
                raise
            connection.settimeout(5)
            with self.lock:
                if self.stopped.is_set():
                    close_connection(connection)
                    return
                self.connections.append(connection)
                worker = threading.Thread(target=self.handle, args=(connection,))
                self.workers.append(worker)
                worker.start()

    def handle(self, connection):
        try:
            self.handler(connection)
        except OSError as error:
            if not self.stopped.is_set():
                self.errors.append(error)
        except Exception as error:
            self.errors.append(error)
        finally:
            close_connection(connection)

    def close(self):
        with self.lock:
            self.stopped.set()
            connections = list(self.connections)
            workers = list(self.workers)
        self.listener.close()
        for connection in connections:
            close_connection(connection)
        self.thread.join(5)
        for worker in workers:
            worker.join(5)
        if self.thread.is_alive() or any(worker.is_alive() for worker in workers):
            raise AssertionError("Servidor de teste não encerrou")
        if self.errors:
            raise AssertionError(self.errors)


class LoadBalancerTests(unittest.TestCase):

    # Preparar as conexões para os testes

    def backend(self, handler=echo):
        backend = TCPBackend(handler)
        self.addCleanup(backend.close)
        return backend

    def balancer(self, select_server, release_server=None, **options):
        balancer = LoadBalancer(("127.0.0.1", 0), select_server,
                                release_server, **options)
        errors = []

        def serve():
            try:
                balancer.serve_forever()
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=serve)
        thread.start()

        def cleanup():
            balancer.shutdown()
            thread.join(5)
            self.assertFalse(thread.is_alive(), "LB não encerrou")
            self.assertEqual(errors, [])

        self.addCleanup(cleanup)
        return balancer

    def client(self, balancer):
        connection = socket.create_connection(balancer.address, timeout=5)
        self.addCleanup(connection.close)
        return connection

    # Conferir dados, concorrência e encerramento das conexões

    def test_binary_stream_larger_than_buffer_and_fragmented_writes(self):
        backend = self.backend()
        balancer = self.balancer(lambda: backend.address)
        client = self.client(balancer)
        payload = bytes(range(256)) * 1024
        for start in range(0, len(payload), 997):
            client.sendall(payload[start:start + 997])
        client.shutdown(socket.SHUT_WR)
        self.assertEqual(receive_all(client), payload)

    def test_client_half_close_still_receives_response(self):
        def respond_after_eof(connection):
            request = receive_all(connection)
            connection.sendall(b"recebido: " + request)

        backend = self.backend(respond_after_eof)
        released = threading.Event()
        balancer = self.balancer(lambda: backend.address,
                                 lambda address: released.set())
        client = self.client(balancer)
        client.sendall(b"pedido")
        client.shutdown(socket.SHUT_WR)
        self.assertEqual(receive_all(client), b"recebido: pedido")
        self.assertTrue(released.wait(5))

    def test_server_can_send_before_client(self):
        def greeting(connection):
            connection.sendall(b"ola")
            echo(connection)

        backend = self.backend(greeting)
        balancer = self.balancer(lambda: backend.address)
        client = self.client(balancer)
        self.assertEqual(receive_exact(client, 3), b"ola")
        client.sendall(b"resposta")
        client.shutdown(socket.SHUT_WR)
        self.assertEqual(receive_all(client), b"resposta")

    def test_multiple_clients_remain_connected_simultaneously(self):
        backend = self.backend()
        balancer = self.balancer(lambda: backend.address)
        barrier = threading.Barrier(8)

        def client_exchange(index):
            with socket.create_connection(balancer.address, timeout=5) as client:
                payload = ("cliente-%s" % index).encode()
                client.sendall(payload)
                response = receive_exact(client, len(payload))
                # Todos precisam receber antes que qualquer cliente encerre.
                barrier.wait(timeout=5)
                client.shutdown(socket.SHUT_WR)
                self.assertEqual(receive_all(client), b"")
                return response

        with ThreadPoolExecutor(max_workers=8) as executor:
            responses = list(executor.map(client_exchange, range(8)))
        self.assertEqual(responses, [("cliente-%s" % i).encode() for i in range(8)])

    def test_selection_occurs_once_per_connection_and_reply_uses_chosen_server(self):
        def tagged(prefix):
            def handler(connection):
                while True:
                    data = connection.recv(65536)
                    if not data:
                        return
                    connection.sendall(prefix + data)
            return handler

        first = self.backend(tagged(b"A:"))
        second = self.backend(tagged(b"B:"))
        choices = iter([first.address, second.address])
        selected = []
        released = []
        lock = threading.Lock()

        def select():
            with lock:
                address = next(choices)
                selected.append(address)
                return address

        def release(address):
            with lock:
                released.append(address)

        balancer = self.balancer(select, release)
        for prefix in [b"A:", b"B:"]:
            with socket.create_connection(balancer.address, timeout=5) as client:
                for data in [b"um", b"dois"]:
                    client.sendall(data)
                    self.assertEqual(receive_exact(client, len(prefix + data)),
                                     prefix + data)
                client.shutdown(socket.SHUT_WR)
                self.assertEqual(receive_all(client), b"")
        balancer.shutdown()
        self.assertEqual(selected, [first.address, second.address])
        self.assertCountEqual(released, selected)

    def test_no_available_server_closes_client(self):
        released = []
        balancer = self.balancer(lambda: None, released.append)
        client = self.client(balancer)
        self.assertEqual(client.recv(1), b"")
        balancer.shutdown()
        self.assertEqual(released, [])

    def test_failed_backend_connection_releases_reservation_once(self):
        # Reservar uma porta sem listen garante conexão recusada, sem reutilização.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            address = unavailable.getsockname()
            released = []
            balancer = self.balancer(lambda: address, released.append)
            client = self.client(balancer)
            self.assertEqual(client.recv(1), b"")
            balancer.shutdown()
            self.assertEqual(released, [address])

    def test_shutdown_closes_idle_connections_and_releases_reservation(self):
        backend = self.backend()
        released = []
        balancer = self.balancer(lambda: backend.address, released.append)
        client = self.client(balancer)
        client.sendall(b"ok")
        self.assertEqual(receive_exact(client, 2), b"ok")
        balancer.shutdown()
        self.assertEqual(client.recv(1), b"")
        self.assertEqual(released, [backend.address])
        balancer.shutdown()  # Encerramento repetido também é seguro.

    def test_client_reset_releases_once_without_interrupting_another_client(self):
        backend = self.backend()
        table = ServerTable(
            filename=None,
            servers=[Server(0, *backend.address, status_server='UP', _enable='YES')]
        )
        released = []
        reset_released = threading.Event()

        def release(address):
            table.release_server(address)
            released.append(address)
            reset_released.set()

        balancer = self.balancer(table.select_server, release)
        interrupted = self.client(balancer)
        survivor = self.client(balancer)
        for client in [interrupted, survivor]:
            client.sendall(b"ok")
            self.assertEqual(receive_exact(client, 2), b"ok")
        # SO_LINGER com prazo zero simula uma queda abrupta, enviando TCP RST.
        interrupted.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        interrupted.close()
        self.assertTrue(reset_released.wait(5))
        self.assertEqual(table.snapshot()["num_total_connections"], 1)
        self.assertEqual(released, [backend.address])
        survivor.sendall(b"continua")
        self.assertEqual(receive_exact(survivor, 8), b"continua")
        balancer.shutdown()
        self.assertEqual(table.snapshot()["num_total_connections"], 0)
        self.assertEqual(released, [backend.address, backend.address])

    def test_shutdown_during_connection_burst_releases_every_reservation(self):
        backend = self.backend()
        table = ServerTable(
            filename=None,
            servers=[Server(0, *backend.address, status_server='UP', _enable='YES')]
        )
        balancer = self.balancer(table.select_server, table.release_server)
        held = self.client(balancer)
        held.sendall(b"ok")
        self.assertEqual(receive_exact(held, 2), b"ok")
        started = threading.Barrier(9)
        burst = threading.Event()
        stopping = threading.Event()
        lock = threading.Lock()
        completed = 0
        errors = []

        def exchange():
            nonlocal completed
            started.wait(timeout=5)
            for _ in range(40):
                try:
                    with socket.create_connection(balancer.address, timeout=1) as client:
                        client.sendall(b"ok")
                        if receive_exact(client, 2) != b"ok":
                            raise AssertionError("LB alterou os dados")
                    with lock:
                        completed += 1
                        if completed >= 16:
                            burst.set()
                except (OSError, AssertionError) as error:
                    if not stopping.is_set():
                        errors.append(error)
                    return

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(exchange) for _ in range(8)]
            try:
                started.wait(timeout=5)
                self.assertTrue(burst.wait(5), "A rajada não começou")
            finally:
                stopping.set()
                balancer.shutdown()
            for future in futures:
                future.result(timeout=5)
        self.assertEqual(errors, [])
        self.assertGreaterEqual(completed, 16)
        self.assertEqual(held.recv(1), b"")
        self.assertEqual(table.snapshot()["num_total_connections"], 0)


    def test_worker_start_failure_rejects_only_the_new_connection(self):
        backend = self.backend()
        table = ServerTable(
            filename=None,
            servers=[Server(0, *backend.address, status_server='UP', _enable='YES')]
        )
        balancer = self.balancer(table.select_server, table.release_server)
        first = self.client(balancer)
        first.sendall(b"ok")
        self.assertEqual(receive_exact(first, 2), b"ok")
        original_start = threading.Thread.start
        failed = threading.Event()

        def start(worker):
            if worker._target == balancer._handle_client and not failed.is_set():
                failed.set()
                raise RuntimeError("Falha temporária ao criar thread")
            return original_start(worker)

        with patch.object(threading.Thread, "start", start):
            rejected = self.client(balancer)
            self.assertEqual(rejected.recv(1), b"")
            self.assertTrue(failed.is_set())
            first.sendall(b"continua")
            self.assertEqual(receive_exact(first, 8), b"continua")
            third = self.client(balancer)
            third.sendall(b"novo")
            self.assertEqual(receive_exact(third, 4), b"novo")
        balancer.shutdown()
        self.assertEqual(table.snapshot()["num_total_connections"], 0)

    def test_round_robin_routes_in_order_and_skips_down_server(self):
        def identify(name):
            def handler(connection):
                request = receive_all(connection)
                connection.sendall(name + request)
            return handler

        first = self.backend(identify(b"A:"))
        down = self.backend(identify(b"DOWN:"))
        second = self.backend(identify(b"B:"))
        servers = [SimpleNamespace(
            ip=backend.address[0],
            port=backend.address[1],
            status_server=status,
            _enable='YES'
        )
                   for backend, status in [(first, "UP"), (down, "DOWN"),
                                           (second, "UP")]]
        policy = RoundRobin(servers)
        balancer = self.balancer(policy.select_server)
        replies = []
        for _ in range(6):
            with socket.create_connection(balancer.address, timeout=5) as client:
                client.sendall(b"pedido")
                client.shutdown(socket.SHUT_WR)
                replies.append(receive_all(client))
        self.assertEqual(replies, [b"A:pedido", b"B:pedido"] * 3)
        with policy.lock:
            for server in servers:
                server.status_server = "DOWN"
        client = self.client(balancer)
        self.assertEqual(client.recv(1), b"")

    def test_round_robin_distributes_concurrent_clients(self):
        def identify(name):
            def handler(connection):
                request = receive_exact(connection, 2)
                connection.sendall(name + request)
                receive_all(connection)
            return handler

        first = self.backend(identify(b"A"))
        second = self.backend(identify(b"B"))
        servers = [SimpleNamespace(
            ip=backend.address[0],
            port=backend.address[1],
            status_server='UP',
            _enable='YES'
        ) for backend in [first, second]]
        policy = RoundRobin(servers)
        balancer = self.balancer(policy.select_server)
        barrier = threading.Barrier(12)

        def exchange(index):
            with socket.create_connection(balancer.address, timeout=5) as client:
                request = ("%02d" % index).encode()
                client.sendall(request)
                response = receive_exact(client, 3)
                self.assertEqual(response[1:], request)
                barrier.wait(timeout=5)
                client.shutdown(socket.SHUT_WR)
                self.assertEqual(receive_all(client), b"")
                return response[:1]

        with ThreadPoolExecutor(max_workers=12) as executor:
            choices = list(executor.map(exchange, range(12)))
        self.assertEqual(Counter(choices), {b"A": 6, b"B": 6})

    def test_table_counts_real_connections_and_cleans_up_after_shutdown(self):
        backend = self.backend()
        table = ServerTable(
            filename=None,
            servers=[Server(0, *backend.address, status_server='UP', _enable='YES')]
        )
        balancer = self.balancer(table.select_server, table.release_server)
        clients = [self.client(balancer) for _ in range(3)]
        for client in clients:
            client.sendall(b"ok")
            self.assertEqual(receive_exact(client, 2), b"ok")
        state = table.snapshot()
        self.assertEqual(state["num_total_connections"], 3)
        self.assertEqual(state["servers"][0]["num_connections"], 3)
        balancer.shutdown()
        state = table.snapshot()
        self.assertEqual(state["num_total_connections"], 0)
        self.assertEqual(state["servers"][0]["num_connections"], 0)

    def test_table_releases_failed_connection_without_losing_other_reservations(self):
        backend = self.backend()
        with socket.socket() as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            table = ServerTable(
                filename=None,
                servers=[
                    Server(0, *backend.address, status_server='UP', _enable='YES'),
                    Server(1, *unavailable.getsockname(), status_server='UP', _enable='YES')
                ]
            )
            released = threading.Event()

            def release(address):
                table.release_server(address)
                released.set()

            balancer = self.balancer(table.select_server, release)
            active_client = self.client(balancer)
            active_client.sendall(b"ok")
            self.assertEqual(receive_exact(active_client, 2), b"ok")
            client = self.client(balancer)
            self.assertEqual(client.recv(1), b"")
            self.assertTrue(released.wait(5))
            state = table.snapshot()
            self.assertEqual(state["num_total_connections"], 1)
            self.assertEqual([s["num_connections"] for s in state["servers"]], [1, 0])
            balancer.shutdown()
            self.assertEqual(table.snapshot()["num_total_connections"], 0)

    def test_least_connections_follows_real_connection_lifetimes(self):
        def identify(name):
            def handler(connection):
                while True:
                    request = connection.recv(65536)
                    if not request:
                        return
                    connection.sendall(name + request)
            return handler

        first = self.backend(identify(b"A:"))
        second = self.backend(identify(b"B:"))
        table = ServerTable(
            filename=None,
            servers=[
                Server(0, *first.address, status_server='UP', _enable='YES'),
                Server(1, *second.address, status_server='UP', _enable='YES')
            ],
            policy_class=LeastConnections
        )
        released = threading.Event()

        def release(address):
            table.release_server(address)
            released.set()

        balancer = self.balancer(table.select_server, release)

        def connect(expected):
            client = self.client(balancer)
            client.sendall(b"ok")
            self.assertEqual(receive_exact(client, 4), expected + b"ok")
            return client

        long_client = connect(b"A:")
        short_client = connect(b"B:")
        other_long_client = connect(b"A:")
        self.assertEqual(table.snapshot()["num_total_connections"], 3)
        short_client.shutdown(socket.SHUT_WR)
        self.assertEqual(receive_all(short_client), b"")
        self.assertTrue(released.wait(5))
        connect(b"B:")  # B ficou livre enquanto as duas conexões de A continuam.
        table.update_status_server(1, "DOWN")
        connect(b"A:")  # B tem menos conexões, mas está DOWN.
        balancer.shutdown()
        self.assertEqual(table.snapshot()["num_total_connections"], 0)


if __name__ == "__main__":
    unittest.main()
