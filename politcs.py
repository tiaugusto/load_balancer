"""Políticas de escolha de servidores para novas conexões do LB."""

from threading import Lock


class RoundRobin:
    """Percorre a lista circularmente, escolhendo somente servidores UP.

    Usa os campos ip, port e status_server da classe Server do Gabriel.
    A tabela/monitoramento deve usar o mesmo lock para alterar a lista ou status.
    """

    def __init__(self, servers, lock=None):
        self.servers = servers
        self.lock = lock if lock is not None else Lock()
        self._next_index = 0

    def select_server(self):
        with self.lock:
            # Visita no máximo uma volta; lista vazia ou toda DOWN retorna None.
            for _ in range(len(self.servers)):
                index = self._next_index % len(self.servers)
                self._next_index = (index + 1) % len(self.servers)
                server = self.servers[index]
                if server.status_server == "UP":
                    return (server.ip, server.port)
            return None
