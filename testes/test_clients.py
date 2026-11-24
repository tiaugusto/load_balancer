# Testes da carga, da duração dos clientes e da parada

import socket
import threading
import time
import unittest
from unittest.mock import patch

from clients import run_load
from load_balance import LoadBalancer
from management.politcs import LeastConnections, RoundRobin
from servers.server_class import Server
from servers.Table import ServerTable
from test_load_balance import TCPBackend, echo


class ClientsTests(unittest.TestCase):

    # Preparar servidor e LB para os testes

    def system(self, policy_class, handler=echo):
        backend = TCPBackend(handler)
        self.addCleanup(backend.close)
        table = ServerTable(
            filename=None,
            servers=[Server(0, *backend.address, status_server='UP', _enable='YES')],
            policy_class=policy_class
        )
        balancer = LoadBalancer(("127.0.0.1", 0), table.select_server,
                                table.release_server)
        errors = []

        def serve():
            try:
                balancer.serve_forever()
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=serve)
        thread.start()

        def close():
            balancer.shutdown()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(table.snapshot()["num_total_connections"], 0)

        self.addCleanup(close)
        return balancer.address

    # Executar as cargas e conferir os resultados

    def test_clients_exchange_bytes_through_lb_with_either_policy(self):
        for policy_class in [RoundRobin, LeastConnections]:
            with self.subTest(policy=policy_class.__name__):
                address = self.system(policy_class)
                report = run_load(address, clients=4, duration=0.2, interval=0.01)
                self.assertEqual(report["successful"], 4)
                self.assertEqual(report["failed"], 0)
                self.assertGreater(report["bytes_sent"], 0)
                for client in report["results"]:
                    self.assertGreater(client["bytes_received"], 0)
                    self.assertIsNone(client["error"])

    def test_variable_durations_keep_clients_alive_for_different_intervals(self):
        address = self.system(LeastConnections)
        report = run_load(address, clients=3, duration=0.15, duration_step=0.1,
                          interval=0.01)
        self.assertEqual(report["successful"], 3)
        durations = [client["requested_seconds"] for client in report["results"]]
        self.assertEqual(durations, [0.15, 0.25, 0.35])
        for client in report["results"]:
            self.assertGreaterEqual(client["elapsed_seconds"], client["requested_seconds"] - 0.01)

    def test_refused_lb_is_reported_for_every_client(self):
        with socket.socket() as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            report = run_load(unavailable.getsockname(), clients=3, duration=0.15)
        self.assertEqual(report["failed"], 3)
        self.assertEqual(report["bytes_received"], 0)
        self.assertTrue(all(client["error"] for client in report["results"]))

    def test_socket_creation_failure_is_reported_for_every_client(self):
        with patch("clients.socket.socket", side_effect=OSError("Sem descritores disponíveis")):
            report = run_load(("127.0.0.1", 8000), clients=3, duration=0.1)
        self.assertEqual(report["failed"], 3)
        self.assertTrue(all("Sem descritores" in client["error"] for client in report["results"]))

    def test_silent_backend_does_not_exceed_the_client_duration_indefinitely(self):
        finish = threading.Event()
        self.addCleanup(finish.set)
        address = self.system(RoundRobin, handler=lambda connection: finish.wait(5))
        started_at = time.monotonic()
        try:
            report = run_load(address, clients=1, duration=0.2, timeout=0.05)
            self.assertLess(time.monotonic() - started_at, 3)
            self.assertEqual(report["failed"], 1)
            self.assertEqual(report["bytes_received"], 0)
        finally:
            finish.set()

    def test_rejects_invalid_load_settings_before_connecting(self):
        for settings in [{"clients": 0}, {"clients": -1}, {"duration": -1},
                         {"duration": float("nan")}, {"duration_step": -1},
                         {"interval": 0}, {"timeout": float("inf")}]:
            arguments = {"clients": 1, "duration": 1}
            arguments.update(settings)
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                run_load(("127.0.0.1", 1), **arguments)

    def test_stop_interrupts_a_blocked_read_even_with_a_long_timeout(self):
        received = threading.Event()
        finish = threading.Event()

        def silent(connection):
            connection.recv(65536)
            received.set()
            finish.wait(5)

        address = self.system(RoundRobin, handler=silent)
        stop = threading.Event()
        results = []
        errors = []

        def load():
            try:
                results.append(run_load(address, clients=1, duration=30, timeout=30,
                                        stop=stop))
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=load)
        thread.start()
        try:
            self.assertTrue(received.wait(5), "Cliente não enviou ao LB")
            stop.set()
            thread.join(2)
            self.assertFalse(thread.is_alive(), "Parada esperou o timeout de 30 segundos")
            self.assertEqual(errors, [])
            self.assertEqual(results[0]["results"][0]["error"], "Carga interrompida")
        finally:
            stop.set()
            finish.set()
            thread.join(5)


if __name__ == "__main__":
    unittest.main()
