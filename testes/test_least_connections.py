# Testes da escolha pelo menor número de conexões abertas

import threading
import unittest
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from management.politcs import LeastConnections
from servers.server_class import Server
from servers.Table import ServerTable


class LeastConnectionsTests(unittest.TestCase):
    def test_picks_lowest_active_count_among_up_servers(self):
        servers = [
            Server(0, '127.0.0.1', 9001, status_server='UP', _enable='YES'),
            Server(1, '127.0.0.1', 9002, status_server='UP', _enable='YES'),
            Server(2, '127.0.0.1', 9003, status_server='DOWN', _enable='YES')
        ]
        servers[0].num_connections = 3
        servers[1].num_connections = 1
        policy = LeastConnections(servers)
        self.assertEqual(policy.select_server(), ("127.0.0.1", 9002))

    def test_ties_follow_registration_order(self):
        servers = [
            Server(8, '127.0.0.1', 9002, status_server='UP', _enable='YES'),
            Server(1, '127.0.0.1', 9001, status_server='UP', _enable='YES')
        ]
        self.assertEqual(LeastConnections(servers).select_server(), ("127.0.0.1", 9002))

    def test_empty_and_all_down_return_none(self):
        for servers in [[], [Server(0, '127.0.0.1', 9001, _enable='YES')]]:
            with self.subTest(servers=servers):
                self.assertIsNone(LeastConnections(servers).select_server())

    def test_released_connections_change_the_next_choice(self):
        table = ServerTable(
            filename=None,
            servers=[
                Server(0, '127.0.0.1', 9001, status_server='UP', _enable='YES'),
                Server(1, '127.0.0.1', 9002, status_server='UP', _enable='YES')
            ],
            policy_class=LeastConnections
        )
        first = table.select_server()
        second = table.select_server()
        third = table.select_server()
        self.assertEqual([first, second, third], [("127.0.0.1", 9001),
                                                  ("127.0.0.1", 9002),
                                                  ("127.0.0.1", 9001)])
        table.release_server(second)
        self.assertEqual(table.select_server(), second)
        table.update_status_server(1, "DOWN")
        self.assertEqual(table.select_server(), first)

    def test_simultaneous_choices_reserve_before_next_client_can_select(self):
        servers = [
            Server(index, '127.0.0.1', 9001 + index, status_server='UP', _enable='YES')
            for index in range(3)
        ]
        table = ServerTable(filename=None, servers=servers, policy_class=LeastConnections)
        barrier = threading.Barrier(12)

        def choose(_):
            barrier.wait(timeout=5)
            return [table.select_server() for _ in range(10)]

        with ThreadPoolExecutor(max_workers=12) as executor:
            choices = list(executor.map(choose, range(12)))
        counts = Counter(address for group in choices for address in group)
        self.assertEqual(counts, {("127.0.0.1", 9001): 40,
                                  ("127.0.0.1", 9002): 40,
                                  ("127.0.0.1", 9003): 40})
        state = table.snapshot()
        self.assertEqual(state["num_total_connections"], 120)
        self.assertEqual([s["num_connections"] for s in state["servers"]], [40, 40, 40])
        for group in choices:
            for address in group:
                table.release_server(address)
        self.assertEqual(table.snapshot()["num_total_connections"], 0)


if __name__ == "__main__":
    unittest.main()
