# Testes da ordem circular e das escolhas concorrentes

import threading
import unittest
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from management.politcs import RoundRobin


def server(port, status="UP"):
    # Registro simples com os campos usados pela política.
    return SimpleNamespace(ip="127.0.0.1", port=port, status_server=status, _enable="YES")


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

if __name__ == "__main__":
    unittest.main()
