# Testes dos processos, da verificação de saúde e da recuperação

import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from clients import run_load
from load_balance import LoadBalancer
from management.monitoramento import Monitor, probe_server
from management.politcs import LeastConnections, RoundRobin
from servers.server_class import Server
from servers.Table import ServerTable
from test_load_balance import TCPBackend


def free_addresses(number):
    reservations = [socket.socket() for _ in range(number)]
    try:
        for connection in reservations:
            connection.bind(("127.0.0.1", 0))
        return [connection.getsockname() for connection in reservations]
    finally:
        for connection in reservations:
            connection.close()


def wait_until(predicate, timeout=6):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        threading.Event().wait(0.01)
    raise AssertionError("Estado esperado não foi observado")


class MonitorTests(unittest.TestCase):

    def monitor(self, table, **options):
        settings = dict(interval=0.05, timeout=0.2, restart_delay=0.25,
                        startup_timeout=1, max_restarts=2)
        settings.update(options)
        monitor = Monitor(table, **settings)
        self.addCleanup(monitor.stop)
        monitor.start()
        return monitor

    def balancer(self, table):
        service = LoadBalancer(("127.0.0.1", 0), table.select_server, table.release_server)
        thread = threading.Thread(target=service.serve_forever)
        thread.start()

        def close():
            service.shutdown()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(table.snapshot()["num_total_connections"], 0)

        self.addCleanup(close)
        return service

    def test_recovers_one_process_without_interrupting_another_client(self):
        for policy in [RoundRobin, LeastConnections]:
            with self.subTest(policy=policy.__name__):
                table = ServerTable(
                    filename=None,
                    servers=[
                        Server(index, *address, _enable='YES')
                        for (index, address) in enumerate(free_addresses(2))
                    ],
                    policy_class=policy
                )
                monitor = self.monitor(table)
                lb = self.balancer(table)
                with socket.create_connection(lb.address, timeout=3) as first:
                    with first.makefile("rb") as replies:
                        first.sendall(b"primeiro")
                        self.assertEqual(replies.readline(), b"OK server=0\n")
                        with socket.create_connection(lb.address, timeout=3) as second:
                            with second.makefile("rb") as other:
                                second.sendall(b"segundo")
                                self.assertEqual(other.readline(), b"OK server=1\n")
                                old_pid = monitor.get_processes()[0]
                                os.kill(old_pid, signal.SIGKILL)
                                wait_until(lambda: table.get_servers()[0].status_server == "DOWN")
                                second.sendall(b"continua")
                                self.assertEqual(other.readline(), b"OK server=1\n")
                                self.assertEqual(replies.read(), b"")
                                wait_until(lambda: table.get_servers()[0].status_server == "UP")
                                self.assertNotEqual(monitor.get_processes()[0], old_pid)
                report = run_load(lb.address, 4, 0.15, interval=0.01)
                self.assertEqual(report["successful"], 4)

    def test_restart_budget_does_not_reset_after_successful_recovery(self):
        table = ServerTable(filename=None, servers=[Server(0, *free_addresses(1)[0], _enable='YES')])
        monitor = self.monitor(table, restart_delay=0.05, max_restarts=1)
        first_pid = monitor.get_processes()[0]
        os.kill(first_pid, signal.SIGKILL)
        wait_until(lambda: monitor.get_processes().get(0) not in (None, first_pid))
        wait_until(lambda: table.get_servers()[0].status_server == "UP")
        os.kill(monitor.get_processes()[0], signal.SIGKILL)
        wait_until(lambda: table.get_servers()[0].status_server == "DOWN")
        threading.Event().wait(0.25)
        self.assertEqual(monitor.get_processes(), {})

    def test_disabled_registration_is_neither_started_nor_recovered(self):
        table = ServerTable(filename=None, servers=[Server(0, *free_addresses(1)[0], _enable="NO")])
        monitor = self.monitor(table, restart_delay=0)
        threading.Event().wait(0.15)
        self.assertEqual(monitor.get_processes(), {})
        self.assertEqual(table.get_servers()[0].status_server, "DOWN")

    def test_manual_start_renews_budget_without_replacing_a_healthy_process(self):
        table = ServerTable(filename=None, servers=[Server(0, *free_addresses(1)[0], _enable='YES')])
        monitor = self.monitor(table, restart_delay=0.05, max_restarts=1)
        first_pid = monitor.get_processes()[0]
        os.kill(first_pid, signal.SIGKILL)
        wait_until(lambda: monitor.get_processes().get(0) not in (None, first_pid))
        wait_until(lambda: table.get_server(0).status_server == "UP")
        recovered_pid = monitor.get_processes()[0]
        self.assertEqual(monitor.execute("start", 0), "OK")
        self.assertEqual(monitor.get_processes()[0], recovered_pid)
        os.kill(recovered_pid, signal.SIGKILL)
        wait_until(lambda: monitor.get_processes().get(0) not in (None, recovered_pid))
        wait_until(lambda: table.get_server(0).status_server == "UP")

    def test_occupied_port_does_not_adopt_or_stop_an_unrelated_service(self):
        backend = TCPBackend(lambda connection: connection.sendall(b"OK server=0\n"))
        self.addCleanup(backend.close)
        table = ServerTable(filename=None, servers=[Server(0, *backend.address, _enable="YES")])
        with self.assertLogs("Monitor", level="WARNING") as logs:
            monitor = self.monitor(table, startup_timeout=0.1, restart_delay=0.02, max_restarts=1)
            wait_until(lambda: any("tentativa=1/1" in line for line in logs.output))
            threading.Event().wait(0.25)
            self.assertEqual(sum("Reinício" in line for line in logs.output), 1)
        self.assertEqual(table.get_servers()[0].status_server, "DOWN")
        monitor.stop()
        self.assertTrue(probe_server(table.get_servers()[0], 0.5))

    def test_all_down_rejects_new_clients_and_stop_reaps_the_children(self):
        table = ServerTable(
            filename=None,
            servers=[Server(i, *address, _enable='YES') for i, address in enumerate(free_addresses(2))]
        )
        monitor = self.monitor(table, max_restarts=0)
        lb = self.balancer(table)
        for pid in monitor.get_processes().values():
            os.kill(pid, signal.SIGKILL)
        wait_until(lambda: all(s.status_server == "DOWN" for s in table.get_servers()))
        with socket.create_connection(lb.address, timeout=3) as client:
            self.assertEqual(client.recv(1), b"")
        monitor.stop()
        self.assertEqual(monitor.get_processes(), {})
        self.assertEqual(table.snapshot()["num_total_connections"], 0)

    def test_probe_handles_fragmented_incomplete_oversized_and_silent_replies(self):
        large_id = 10 ** 128
        cases = [
            (0, [b"OK ", b"server=", b"0\n"], 0.01, True),
            (0, [b"OK ", b"server=", b"0\n"], 0.15, False),
            (0, [b"OK server=0\nOK server=0\n"], 0, True),
            (0, [b"OK server=0"], 0, False),
            (0, [b"x" * 128], 0, False),
            (0, [], 0, False),
            (large_id, [("OK server=%s\n" % large_id).encode("ascii")], 0, True)
        ]
        for server_id, parts, delay, expected in cases:
            with self.subTest(server_id=server_id, parts=parts, delay=delay):
                finish = threading.Event()

                def respond(connection):
                    connection.recv(128)
                    for part in parts:
                        connection.sendall(part)
                        if finish.wait(delay):
                            return
                    if not parts:
                        finish.wait(5)

                backend = TCPBackend(respond)
                try:
                    server = Server(server_id, *backend.address, _enable="YES")
                    started = time.monotonic()
                    self.assertEqual(probe_server(server, timeout=0.2), expected)
                    self.assertLess(time.monotonic() - started, 1)
                finally:
                    finish.set()
                    backend.close()


class SystemCLITests(unittest.TestCase):

    def launch(self, number=2, policy="round-robin", wait_for_lb=True, lb_port=None,
               menu=False, script_name="run_system.py", relative_config=False):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        addresses = free_addresses(number + 1)
        path = Path(directory.name, "servers.json")
        path.write_text(json.dumps({"servers": [
            {"id": i, "ip": address[0], "port": address[1], "_enable": "YES"}
            for i, address in enumerate(addresses[:number])]}), encoding="utf-8")
        script = Path(__file__).resolve().parents[1] / script_name
        config_argument = path.name if relative_config else str(path)
        command = [sys.executable, str(script), "--servers-file", config_argument,
                   "--port", str(lb_port or addresses[-1][1]), "--policy", policy,
                   "--check-interval", "0.05", "--probe-timeout", "0.1"]
        if menu:
            command.append("--menu")
        process = subprocess.Popen(command, stdin=subprocess.PIPE if menu else None,
                                   cwd=directory.name, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        ready, created = threading.Event(), threading.Event()
        output, pids = [], []

        def read():
            for line in process.stderr:
                output.append(line)
                match = re.search(r"PID=(\d+)", line)
                if match:
                    pids.append(int(match[1]))
                    created.set()
                if "LB ouvindo em" in line:
                    ready.set()

        reader = threading.Thread(target=read)
        reader.start()

        def close():
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(6)
                except subprocess.TimeoutExpired:
                    process.kill()
            process.wait(3)
            reader.join(3)
            if process.stdin is not None:
                process.stdin.close()
            process.stdout.close()
            process.stderr.close()
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

        self.addCleanup(close)
        self.assertTrue((ready if wait_for_lb else created).wait(8), "".join(output))
        return process, reader, pids, addresses[-1], output

    def assert_children_gone(self, pids):
        for pid in pids:
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    def test_cli_load_and_shutdown_with_either_policy(self):
        for policy in ["round-robin", "least-connections"]:
            with self.subTest(policy=policy):
                process, reader, pids, address, output = self.launch(policy=policy)
                script = Path(__file__).resolve().parents[1] / "clients.py"
                load = subprocess.run([sys.executable, str(script), "--host", address[0],
                                       "--port", str(address[1]), "-M", "6", "-K", "0.2",
                                       "--duration-step", "0.02", "--interval", "0.01"],
                                      cwd=script.parent, capture_output=True, text=True, timeout=8)
                self.assertEqual(load.returncode, 0, load.stderr)
                result = json.loads(load.stdout)
                self.assertEqual(result["successful"], 6)
                process.send_signal(signal.SIGINT)
                self.assertEqual(process.wait(8), 0, "".join(output))
                reader.join(3)
                self.assert_children_gone(pids)
                self.assertIn("total de conexões=0", "".join(output))

    def test_cli_resolves_config_relative_to_the_execution_directory(self):
        process, reader, pids, address, output = self.launch(number=1, relative_config=True)
        self.assertEqual(run_load(address, 1, 0.1)["successful"], 1)
        process.send_signal(signal.SIGINT)
        self.assertEqual(process.wait(8), 0, "".join(output))
        reader.join(3)
        self.assert_children_gone(pids)

    def test_signal_during_startup_still_reaps_the_started_process(self):
        process, reader, pids, _, output = self.launch(number=5, wait_for_lb=False)
        process.send_signal(signal.SIGTERM)
        self.assertEqual(process.wait(8), 0, "".join(output))
        reader.join(3)
        self.assert_children_gone(pids)

    def test_repeated_signals_do_not_interrupt_cleanup_of_a_frozen_child(self):
        process, reader, pids, _, output = self.launch(number=1)
        os.kill(pids[0], signal.SIGSTOP)
        process.send_signal(signal.SIGINT)
        threading.Event().wait(0.1)
        process.send_signal(signal.SIGTERM)
        self.assertEqual(process.wait(8), 0, "".join(output))
        reader.join(3)
        self.assert_children_gone(pids)

    def test_lb_startup_error_reaps_all_created_servers(self):
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen()
            process, reader, pids, _, output = self.launch(wait_for_lb=False,
                                                         lb_port=occupied.getsockname()[1])
            self.assertEqual(process.wait(8), 1)
            reader.join(3)
            self.assert_children_gone(pids)
            self.assertNotIn("Traceback", "".join(output))

    def test_menu_changes_the_table_used_by_lb_and_can_edit_the_address(self):
        process, reader, pids, address, output = self.launch(number=1, menu=True,
                                                            script_name="menu.py")
        path = Path(process.args[process.args.index("--servers-file") + 1])

        def send(commands):
            process.stdin.write(commands)
            process.stdin.flush()

        self.assertEqual(run_load(address, 1, 0.1)["successful"], 1)
        send("2\n7\n0\n1\n")
        wait_until(lambda: json.loads(path.read_text())["servers"][0].get("_enable") == "NO")
        self.assertEqual(run_load(address, 1, 0.1)["failed"], 1)
        port = free_addresses(1)[0][1]
        send("3\n7\n0\n13\n7\n0\n127.0.0.2\n13\n6\n0\n%s\n3\n6\n0\n2\n6\n0\n" % port)
        wait_until(lambda: json.loads(path.read_text())["servers"][0].get("_enable") == "YES")
        wait_until(lambda: json.loads(path.read_text())["servers"][0]["ip"] == "127.0.0.2")
        threading.Event().wait(0.15)
        self.assertEqual(run_load(address, 1, 0.1)["successful"], 1)
        send("15\n")
        self.assertEqual(process.wait(8), 0, "".join(output))
        reader.join(3)
        self.assert_children_gone(pids)
        self.assertIn("ENABLE: NO", process.stdout.read())

    def test_menu_ctrlc_while_waiting_input_stops_children(self):
        process, reader, pids, _, output = self.launch(number=1, menu=True)
        process.send_signal(signal.SIGINT)
        self.assertEqual(process.wait(8), 0, "".join(output))
        reader.join(3)
        self.assert_children_gone(pids)

    def test_menu_reloads_edited_json_and_serves_the_added_backend(self):
        process, reader, pids, address, output = self.launch(number=1, menu=True)
        path = Path(process.args[process.args.index("--servers-file") + 1])
        records = json.loads(path.read_text())["servers"]
        records[0]["_enable"] = "NO"
        records.append({"id": 7, "ip": "127.0.0.1", "port": free_addresses(1)[0][1],
                        "_enable": "YES"})
        path.write_text(json.dumps({"servers": records}))
        process.stdin.write("13\n8\n")
        process.stdin.flush()
        wait_until(lambda: len(pids) == 2)
        wait_until(lambda: "Servidor 7 marcado UP" in Path(path.parent, "logs/Server.log").read_text())
        with socket.create_connection(address, timeout=3) as client:
            with client.makefile("rb") as replies:
                client.sendall(b"apos reload")
                self.assertEqual(replies.readline(), b"OK server=7\n")
        process.stdin.write("15\n")
        process.stdin.flush()
        self.assertEqual(process.wait(8), 0, "".join(output))
        reader.join(3)
        self.assert_children_gone(pids)
        self.assertIn("Cadastro recarregado", process.stdout.read())

    def test_menu_exit_with_an_active_connection_releases_the_reservation(self):
        process, reader, pids, address, output = self.launch(number=1, menu=True)
        with socket.create_connection(address, timeout=3) as client:
            with client.makefile("rb") as replies:
                client.sendall(b"ativo")
                self.assertEqual(replies.readline(), b"OK server=0\n")
                process.stdin.write("15\n")
                process.stdin.flush()
                self.assertEqual(process.wait(8), 0, "".join(output))
                self.assertEqual(replies.read(), b"")
        reader.join(3)
        self.assert_children_gone(pids)
        self.assertIn("total de conexões=0", "".join(output))
