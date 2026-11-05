import socket
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from load_balance import LoadBalancer, close_connection


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
    """Servidor de teste com sockets reais, independente da implementação do LB."""

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

    def test_connection_limit_does_not_interrupt_existing_client(self):
        backend = self.backend()
        balancer = self.balancer(lambda: backend.address, max_connections=1)
        first = self.client(balancer)
        first.sendall(b"primeiro")
        self.assertEqual(receive_exact(first, 8), b"primeiro")
        second = self.client(balancer)
        self.assertEqual(second.recv(1), b"")
        first.sendall(b"continua")
        self.assertEqual(receive_exact(first, 8), b"continua")


if __name__ == "__main__":
    unittest.main()
