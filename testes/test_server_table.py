# Testes do cadastro inicial, das reservas e dos contadores compartilhados

import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from management.politcs import LeastConnections, RoundRobin
from servers.server_class import Server
from servers.Table import ServerTable


class ServerTableTests(unittest.TestCase):

    def test_server_keeps_gabriel_positional_arguments_and_writable_enable(self):
        server = Server(7, '127.0.0.1', 9001, 'UP', 'YES')
        policy = RoundRobin([server])
        self.assertEqual(policy.select_server(), server.get_address())
        server._enable = "NO"
        self.assertIsNone(policy.select_server())
        server._enable = "YES"
        self.assertEqual(policy.select_server(), server.get_address())
        self.assertEqual(Server(8, '127.0.0.1', 9002)._enable, "NO")
        with self.assertRaises(TypeError):
            Server(8)
        with self.assertRaises(TypeError):
            Server(8, "127.0.0.1", 9002, enable=True)

    def test_reservation_release_and_status_preserve_live_counts(self):
        table = ServerTable(
            filename=None,
            servers=[
                Server(0, '127.0.0.1', 9001, status_server='UP', _enable='YES'),
                Server(1, '127.0.0.1', 9002, status_server='UP', _enable='YES')
            ]
        )
        first = table.select_server()
        second = table.select_server()
        table.update_status_server(0, "DOWN")
        state = table.snapshot()
        self.assertEqual(state["num_total_connections"], 2)
        self.assertEqual([server["num_connections"] for server in state["servers"]], [1, 1])
        self.assertEqual(table.select_server(), second)
        table.release_server(second)
        table.release_server(second)
        table.release_server(first)
        self.assertEqual(table.snapshot()["num_total_connections"], 0)
        with self.assertRaises(ValueError):
            table.release_server(first)
        self.assertEqual(table.snapshot()["num_total_connections"], 0)

    def test_snapshot_does_not_allow_mutating_shared_state(self):
        table = ServerTable(
            filename=None,
            servers=[Server(0, '127.0.0.1', 9001, status_server='UP', _enable='YES')]
        )
        state = table.snapshot()
        state["servers"][0]["status_server"] = "DOWN"
        state["num_total_connections"] = 999
        self.assertEqual(table.select_server(), ("127.0.0.1", 9001))
        self.assertEqual(table.snapshot()["num_total_connections"], 1)

    def test_concurrent_reservations_are_counted_until_released(self):
        table = ServerTable(
            filename=None,
            servers=[
                Server(0, '127.0.0.1', 9001, status_server='UP', _enable='YES'),
                Server(1, '127.0.0.1', 9002, status_server='UP', _enable='YES')
            ]
        )
        arrived = threading.Barrier(13)
        release = threading.Event()

        def reserve(_):
            address = table.select_server()
            try:
                arrived.wait(timeout=5)
                if not release.wait(5):
                    raise AssertionError("Teste não autorizou liberação")
            finally:
                table.release_server(address)

        with ThreadPoolExecutor(max_workers=12) as executor:
            pending = [executor.submit(reserve, index) for index in range(12)]
            try:
                arrived.wait(timeout=5)
                state = table.snapshot()
                self.assertEqual(state["num_total_connections"], 12)
                self.assertEqual([s["num_connections"] for s in state["servers"]], [6, 6])
            finally:
                release.set()
            for result in pending:
                result.result(timeout=5)
        self.assertEqual(table.snapshot()["num_total_connections"], 0)

    def test_rejects_ambiguous_ids_or_endpoints(self):
        for servers in [
            [
                Server(0, '127.0.0.1', 9001, _enable='YES'),
                Server(0, '127.0.0.1', 9002, _enable='YES')
            ],
            [
                Server(0, '127.0.0.1', 9001, _enable='YES'),
                Server(1, '127.0.0.1', 9001, _enable='YES')
            ]
        ]:
            with self.subTest(servers=servers), self.assertRaises(ValueError):
                ServerTable(filename=None, servers=servers)

    def test_loads_gabriel_schema_without_reusing_counts_or_writing_the_file(self):
        data = {"num_total_connections": 8, "servers": [
            {"id": 0, "ip": "127.0.0.1", "port": 9001, "status_server": "UP",
             "_enable": "YES", "num_connections": 8},
            {"id": 1, "ip": "127.0.0.1", "port": 9002, "_enable": "NO"}]}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "servers.json")
            path.write_text(json.dumps(data), encoding="utf-8")
            original = path.read_bytes()
            table = ServerTable(path)
            self.assertEqual(table.snapshot()["num_total_connections"], 0)
            self.assertEqual([s.status_server for s in table.get_servers()], ["DOWN", "DOWN"])
            table.update_status_server(0, "UP")
            address = table.select_server()
            self.assertEqual(address, ("127.0.0.1", 9001))
            table.release_server(address)
            self.assertEqual(path.read_bytes(), original)

    def test_both_policies_skip_disabled_and_down_servers(self):
        for policy in [RoundRobin, LeastConnections]:
            with self.subTest(policy=policy.__name__):
                table = ServerTable(
                    filename=None,
                    servers=[
                        Server(0, '127.0.0.1', 9001, status_server='UP', _enable='NO'),
                        Server(1, '127.0.0.1', 9002, _enable='YES'),
                        Server(2, '127.0.0.1', 9003, status_server='UP', _enable='YES')
                    ],
                    policy_class=policy
                )
                self.assertEqual(table.select_server(), ("127.0.0.1", 9003))
                table.update_status_server(2, "DOWN")
                self.assertIsNone(table.select_server())
                table.release_server(("127.0.0.1", 9003))
                self.assertEqual(table.snapshot()["num_total_connections"], 0)

    def test_rejects_invalid_configuration(self):
        cases = [{}, [], {"servers": [1]}, {"servers": [{"id": 0}]},
                 {"servers": [{"id": 0, "ip": "bad", "port": 9001}]},
                 {"servers": [{"id": 0, "ip": "127.0.0.1", "port": True}]},
                 {"servers": [{"id": 0, "ip": "127.0.0.1", "port": 9001, "enable": "false"}]}]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "servers.json")
            for data in cases:
                path.write_text(json.dumps(data), encoding="utf-8")
                with self.subTest(data=data), self.assertRaises(ValueError):
                    ServerTable(path)
