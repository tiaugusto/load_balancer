import subprocess
import sys
import threading
import unittest
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

from politcs import RoundRobin


def server(port, status="UP"):
    # Mesmos campos utilizados da classe Server; sem depender da branch Gabriel.
    return SimpleNamespace(ip="127.0.0.1", port=port, status_server=status)


class RoundRobinTests(unittest.TestCase):
    def test_visits_servers_in_order_and_wraps(self):
        policy = RoundRobin([server(9001), server(9002), server(9003)])
        chosen = [policy.select_server() for _ in range(7)]
        self.assertEqual(chosen, [("127.0.0.1", port) for port in
                                 [9001, 9002, 9003, 9001, 9002, 9003, 9001]])

    def test_down_servers_do_not_break_the_rotation(self):
        policy = RoundRobin([server(9001, "DOWN"), server(9002),
                             server(9003, "DOWN"), server(9004)])
        chosen = [policy.select_server() for _ in range(6)]
        self.assertEqual(chosen, [("127.0.0.1", port) for port in
                                 [9002, 9004, 9002, 9004, 9002, 9004]])

    def test_empty_or_all_down_returns_none(self):
        for servers in [[], [server(9001, "DOWN"), server(9002, "DOWN")]]:
            with self.subTest(servers=servers):
                policy = RoundRobin(servers)
                self.assertIsNone(policy.select_server())

    def test_only_one_up_server_is_always_selected(self):
        policy = RoundRobin([server(9001, "DOWN"), server(9002)])
        for _ in range(4):
            self.assertEqual(policy.select_server(), ("127.0.0.1", 9002))

    def test_status_changes_use_the_shared_table_lock(self):
        servers = [server(9001), server(9002), server(9003)]
        table_lock = threading.Lock()
        policy = RoundRobin(servers, lock=table_lock)
        self.assertEqual(policy.select_server(), ("127.0.0.1", 9001))
        with table_lock:
            servers[1].status_server = "DOWN"
        self.assertEqual(policy.select_server(), ("127.0.0.1", 9003))
        self.assertEqual(policy.select_server(), ("127.0.0.1", 9001))
        with table_lock:
            servers[1].status_server = "UP"
        self.assertEqual(policy.select_server(), ("127.0.0.1", 9002))

    def test_recovers_after_every_server_was_down(self):
        servers = [server(9001, "DOWN"), server(9002, "DOWN")]
        policy = RoundRobin(servers)
        self.assertIsNone(policy.select_server())
        with policy.lock:
            servers[1].status_server = "UP"
        self.assertEqual(policy.select_server(), ("127.0.0.1", 9002))

    def test_concurrent_selection_is_even_among_up_servers(self):
        policy = RoundRobin([server(9001), server(9002, "DOWN"), server(9003)])
        barrier = threading.Barrier(12)

        def choose(_):
            barrier.wait(timeout=5)
            return [policy.select_server() for _ in range(25)]

        with ThreadPoolExecutor(max_workers=12) as executor:
            selections = list(executor.map(choose, range(12)))
        counts = Counter(address for group in selections for address in group)
        self.assertEqual(counts, {("127.0.0.1", 9001): 150,
                                  ("127.0.0.1", 9003): 150})

    def test_cli_rejects_invalid_server_configuration(self):
        cases = [
            [],
            ["--server", "127.0.0.1"],
            ["--server", "127.0.0.1:70000"],
            ["--server", "not-an-ip:9001"],
            ["--server-ip", "127.0.0.1"],
            ["--server", "127.0.0.1:9001", "--server-port", "9002"],
            ["--server", "127.0.0.1:9001", "--server", "127.0.0.1:9001"],
            ["--policy", "unknown", "--server", "127.0.0.1:9001"],
        ]
        root = Path(__file__).resolve().parents[1]
        for arguments in cases:
            with self.subTest(arguments=arguments):
                result = subprocess.run([sys.executable, "load_balance.py"] + arguments,
                                        cwd=root, capture_output=True, text=True,
                                        timeout=5)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
