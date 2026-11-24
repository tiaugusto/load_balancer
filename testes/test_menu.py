# Operações administrativas sobre a tabela real e os processos do monitor.

import json
import os
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import signal
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from clients import run_load
from load_balance import LoadBalancer
from management.monitoramento import Monitor
from management.politcs import LeastConnections, RoundRobin
from menu import run_menu
from servers.server_class import Server
from servers.Table import ServerTable
from test_monitor import free_addresses, wait_until


class AdministrationTests(unittest.TestCase):

    def system(self, servers, policy=RoundRobin):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name, "servers.json")
        path.write_text(json.dumps({"servers": []}), encoding="utf-8")
        table = ServerTable(filename=path, servers=servers, policy_class=policy)
        monitor = Monitor(table, interval=0.05, timeout=0.2, restart_delay=0.2)
        self.addCleanup(monitor.stop)
        monitor.start()
        lb = LoadBalancer(("127.0.0.1", 0), table.select_server, table.release_server)
        thread = threading.Thread(target=lb.serve_forever)
        thread.start()

        def close():
            lb.shutdown()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(table.snapshot()["num_total_connections"], 0)

        self.addCleanup(close)
        return table, monitor, lb, path

    def test_create_enable_disable_edit_and_delete_with_real_connections(self):
        for policy in [RoundRobin, LeastConnections]:
            with self.subTest(policy=policy.__name__):
                table, monitor, lb, path = self.system([], policy)
                address = free_addresses(1)[0]
                monitor.execute("create", *address)
                self.assertEqual(table.get_server(0)._enable, "NO")
                monitor.execute("start", 0)
                self.assertEqual(table.get_server(0).status_server, "UP")
                self.assertIsNone(table.select_server())
                monitor.execute("enable", 0)
                wait_until(lambda: table.get_server(0).status_server == "UP")
                with socket.create_connection(lb.address, timeout=3) as client:
                    with client.makefile("rb") as replies:
                        client.sendall(b"pedido")
                        self.assertEqual(replies.readline(), b"OK server=0\n")
                        monitor.execute("disable", 0)
                        self.assertIsNone(table.select_server())
                        for operation in ["stop", "delete"]:
                            with self.subTest(operation=operation), self.assertRaises(ValueError):
                                monitor.execute(operation, 0)
                        client.sendall(b"continua")
                        self.assertEqual(replies.readline(), b"OK server=0\n")
                wait_until(lambda: table.snapshot()["num_total_connections"] == 0)
                monitor.execute("stop", 0)
                new_port = free_addresses(1)[0][1]
                monitor.execute("set_ip", 0, "127.0.0.2")
                monitor.execute("set_port", 0, new_port)
                self.assertEqual(table.get_server(0).get_address(), ("127.0.0.2", new_port))
                monitor.execute("start", 0)
                monitor.execute("enable", 0)
                wait_until(lambda: table.get_server(0).status_server == "UP")
                result = run_load(lb.address, 2, 0.15, interval=0.01)
                self.assertEqual(result["successful"], 2)
                wait_until(lambda: table.snapshot()["num_total_connections"] == 0)
                monitor.execute("delete", 0)
                self.assertEqual(table.get_servers(), [])
                self.assertEqual(monitor.get_processes(), {})
                self.assertEqual(json.loads(path.read_text())["servers"], [])

    def test_bulk_operations_refuse_partial_stop_while_a_client_is_active(self):
        servers = [Server(i, *a, _enable="NO") for i, a in enumerate(free_addresses(2))]
        table, monitor, lb, path = self.system(servers)
        monitor.execute("start_all")
        monitor.execute("enable_all")
        wait_until(lambda: all(s.status_server == "UP" for s in table.get_servers()))
        with socket.create_connection(lb.address, timeout=3) as client:
            with client.makefile("rb") as replies:
                client.sendall(b"ativo")
                self.assertEqual(replies.readline(), b"OK server=0\n")
                with self.assertRaises(ValueError):
                    monitor.execute("stop_all")
                self.assertEqual(len(monitor.get_processes()), 2)
                self.assertTrue(all(s._enable == "YES" for s in table.get_servers()))
                monitor.execute("disable_all")
                client.sendall(b"ainda ativo")
                self.assertEqual(replies.readline(), b"OK server=0\n")
        wait_until(lambda: table.snapshot()["num_total_connections"] == 0)
        monitor.execute("stop_all")
        self.assertEqual(monitor.get_processes(), {})
        self.assertTrue(all(s._enable == "NO" for s in table.get_servers()))
        monitor.execute("delete_all")
        self.assertEqual(table.get_servers(), [])

    def test_bad_addresses_and_unknown_ids_leave_registration_unchanged(self):
        servers = [Server(i, *a, _enable="NO") for i, a in enumerate(free_addresses(2))]
        servers.append(Server(2, '192.0.2.1', 9001, _enable='NO'))
        table, monitor, _, _ = self.system(servers)
        original = table.snapshot()
        duplicate = table.get_server(1).get_address()
        for action, args in [("create", duplicate), ("set_ip", (0, "invalid")),
                             ("set_ip", (0, "192.0.2.1")), ("set_port", (0, 65536)),
                             ("enable", (999,)), ("enable", (2,)),
                             ("enable_all", ()), ("start_all", ())]:
            with self.subTest(action=action, args=args), self.assertRaises(ValueError):
                monitor.execute(action, *args)
        self.assertEqual(table.snapshot(), original)

    def test_admin_waits_for_health_cycle_without_blocking_lb_reservations(self):
        table, monitor, _, _ = self.system([Server(0, *free_addresses(1)[0], _enable="YES")])
        entered, release, changed = threading.Event(), threading.Event(), threading.Event()
        from management.monitoramento import probe_server

        def slow_probe(server, timeout):
            entered.set()
            if not release.wait(3):
                raise AssertionError("Verificação não foi liberada pelo teste")
            return probe_server(server, timeout)

        cycle = threading.Thread(target=monitor.check_once)
        admin = threading.Thread(target=lambda: (monitor.execute("disable", 0), changed.set()))
        with patch("management.monitoramento.probe_server", side_effect=slow_probe):
            cycle.start()
            try:
                self.assertTrue(entered.wait(3))
                admin.start()
                self.assertFalse(changed.wait(0.05))
                address = table.select_server()
                self.assertIsNotNone(address)
                table.release_server(address)
            finally:
                release.set()
                cycle.join(3)
                if admin.ident is not None:
                    admin.join(3)
        self.assertFalse(cycle.is_alive())
        self.assertFalse(admin.is_alive())
        self.assertTrue(changed.is_set())
        self.assertEqual(table.get_server(0)._enable, "NO")

    def test_atomic_save_failure_preserves_file_and_reports_memory_change(self):
        table, monitor, _, path = self.system([Server(0, *free_addresses(1)[0], _enable="NO")])
        before = path.read_bytes()
        with patch("servers.Table.os.replace", side_effect=OSError("Disco indisponível")):
            result = monitor.execute("disable", 0)
        self.assertIn("falha ao salvar", result)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(list(path.parent.iterdir()), [path])

    def test_reload_adds_servers_without_losing_clients_or_counters(self):
        for policy in [RoundRobin, LeastConnections]:
            with self.subTest(policy=policy.__name__):
                table, monitor, lb, path = self.system([Server(0, *free_addresses(1)[0], _enable='YES')], policy)
                address = table.get_server(0).get_address()
                added, disabled = free_addresses(2)
                with socket.create_connection(lb.address, timeout=3) as client:
                    with client.makefile("rb") as replies:
                        client.sendall(b"antes")
                        self.assertEqual(replies.readline(), b"OK server=0\n")
                        pid = monitor.get_processes()[0]
                        records = [{"id": id, "ip": ip, "port": port, "_enable": enabled,
                                    "status_server": "DOWN", "num_connections": 999}
                                   for id, (ip, port), enabled in
                                   [(0, address, "YES"), (1, added, "YES"), (2, disabled, "NO")]]
                        path.write_text(json.dumps({"num_total_connections": 999, "servers": records}))
                        saved = path.read_bytes()
                        self.assertEqual(monitor.execute("reload"), "Cadastro recarregado")
                        self.assertEqual(path.read_bytes(), saved)
                        self.assertEqual(monitor.get_processes()[0], pid)
                        self.assertEqual(set(monitor.get_processes()), {0, 1})
                        self.assertEqual(table.snapshot()["num_total_connections"], 1)
                        self.assertEqual(table.get_server(0).num_connections, 1)
                        self.assertEqual(table.get_server(1).num_connections, 0)
                        self.assertEqual(table.get_server(2).status_server, "DOWN")
                        client.sendall(b"depois")
                        self.assertEqual(replies.readline(), b"OK server=0\n")
                        expected = [0, 1] if policy is RoundRobin else [1]
                        for id in expected:
                            with socket.create_connection(lb.address, timeout=3) as second:
                                with second.makefile("rb") as other:
                                    second.sendall(b"novo")
                                    self.assertEqual(other.readline(), ("OK server=%s\n" % id).encode())

    def test_reload_removes_and_changes_idle_servers_and_reuses_their_ports(self):
        table, monitor, lb, path = self.system([Server(i, *a, _enable='YES') for i, a in enumerate(free_addresses(2))])
        previous = monitor.get_processes()
        new_address = free_addresses(1)[0]
        reused = table.get_server(1).get_address()
        records = [{"id": id, "ip": ip, "port": port, "_enable": "YES"}
                   for id, (ip, port) in [(0, new_address), (2, reused)]]
        path.write_text(json.dumps({"servers": records}))
        monitor.execute("reload")
        self.assertEqual(set(monitor.get_processes()), {0, 2})
        self.assertNotEqual(monitor.get_processes()[0], previous[0])
        self.assertEqual(table.get_server(0).get_address(), new_address)
        self.assertTrue(all(s.status_server == "UP" for s in table.get_servers()))
        for id in [0, 2]:
            with socket.create_connection(lb.address, timeout=3) as client:
                with client.makefile("rb") as replies:
                    client.sendall(b"novo cadastro")
                    self.assertEqual(replies.readline(), ("OK server=%s\n" % id).encode())
        wait_until(lambda: table.snapshot()["num_total_connections"] == 0)
        path.write_text('{"servers": []}')
        monitor.execute("reload")
        self.assertEqual(table.get_servers(), [])
        self.assertEqual(monitor.get_processes(), {})
        self.assertIsNone(table.select_server())

    def test_bad_reload_or_active_address_change_leaves_all_runtime_state_unchanged(self):
        table, monitor, lb, path = self.system([Server(0, *free_addresses(1)[0], _enable="YES")])
        base = {"id": 0, "ip": "127.0.0.1", "port": table.get_server(0).port, "_enable": "YES"}
        changed = dict(base, port=free_addresses(1)[0][1])
        candidates = ["{", "null", json.dumps({"servers": "invalid"}),
                      json.dumps({"servers": [base, base]}),
                      json.dumps({"servers": [dict(base, _enable="invalid")]}),
                      json.dumps({"servers": [dict(base, ip="192.0.2.1")]}),
                      json.dumps({"servers": [changed]}), '{"servers": []}']
        with socket.create_connection(lb.address, timeout=3) as client:
            with client.makefile("rb") as replies:
                client.sendall(b"ativo")
                self.assertEqual(replies.readline(), b"OK server=0\n")
                state, processes = table.snapshot(), monitor.get_processes()
                for data in candidates:
                    with self.subTest(data=data):
                        path.write_text(data)
                        with self.assertRaises(ValueError):
                            monitor.execute("reload")
                        self.assertEqual(table.snapshot(), state)
                        self.assertEqual(monitor.get_processes(), processes)
                        self.assertEqual(path.read_text(), data)
                path.unlink()
                with self.assertRaises(OSError):
                    monitor.execute("reload")
                self.assertEqual(table.snapshot(), state)
                self.assertEqual(monitor.get_processes(), processes)
                client.sendall(b"continua")
                self.assertEqual(replies.readline(), b"OK server=0\n")

    def test_reload_enable_flag_preserves_existing_connections(self):
        table, monitor, lb, path = self.system([Server(0, *free_addresses(1)[0], _enable="YES")])
        with socket.create_connection(lb.address, timeout=3) as client:
            with client.makefile("rb") as replies:
                client.sendall(b"ativo")
                self.assertEqual(replies.readline(), b"OK server=0\n")
                pid = monitor.get_processes()[0]
                for enabled in ["NO", "YES"]:
                    record = {"id": 0, "ip": "127.0.0.1", "port": table.get_server(0).port,
                              "_enable": enabled}
                    path.write_text(json.dumps({"servers": [record]}))
                    monitor.execute("reload")
                    self.assertEqual(monitor.get_processes()[0], pid)
                    self.assertEqual(table.get_server(0)._enable, enabled)
                    if enabled == "NO":
                        self.assertIsNone(table.select_server())
                    client.sendall(b"continua")
                    self.assertEqual(replies.readline(), b"OK server=0\n")
                self.assertEqual(run_load(lb.address, 1, 0.1)["successful"], 1)

    def test_reload_unchanged_server_does_not_renew_its_restart_budget(self):
        table, monitor, _, path = self.system([Server(0, *free_addresses(1)[0], _enable="YES")])
        monitor.max_restarts = 1
        table.saveTable()
        first = monitor.get_processes()[0]
        os.kill(first, signal.SIGKILL)
        wait_until(lambda: monitor.get_processes().get(0) not in (None, first))
        wait_until(lambda: table.get_server(0).status_server == "UP")
        recovered = monitor.get_processes()[0]
        monitor.execute("reload")
        os.kill(recovered, signal.SIGKILL)
        wait_until(lambda: table.get_server(0).status_server == "DOWN")
        threading.Event().wait(0.4)
        self.assertEqual(monitor.get_processes(), {})

    def test_yes_no_schema_uses_only_gabriel_enable_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "servers.json")
            base = {"id": 7, "ip": "127.0.0.1", "port": 9001}
            for flag in ["YES", "NO"]:
                path.write_text(json.dumps({"servers": [dict(base, _enable=flag)]}))
                table = ServerTable(path)
                self.assertEqual(table.get_server(7)._enable, flag)
            for record in [dict(base, _enable="maybe"), dict(base, _enable="NO", enable=True)]:
                path.write_text(json.dumps({"servers": [record]}))
                with self.assertRaises(ValueError):
                    ServerTable(path)

    def test_match_case_menu_dispatches_registration_and_process_actions(self):
        table, monitor, _, _ = self.system([])
        port = free_addresses(1)[0][1]
        commands = ["4", "6", "127.0.0.1", str(port),
                    "3", "6", "0", "2", "6", "0", "1",
                    "2", "8", "2", "9", "3", "8", "3", "9",
                    "4", "8", "13", "8", "5", "2", "13", "14", "15"]
        output = StringIO()
        with patch("menu.read_line", side_effect=commands), redirect_stdout(output):
            run_menu(table, monitor, lambda: False)
        self.assertNotIn("Operação recusada", output.getvalue())
        self.assertNotIn("Opção inválida", output.getvalue())
        self.assertIn("Cadastro recarregado", output.getvalue())
        self.assertIn("GRR20232334", output.getvalue())
        self.assertEqual(table.get_servers(), [])
        self.assertEqual(monitor.get_processes(), {})
