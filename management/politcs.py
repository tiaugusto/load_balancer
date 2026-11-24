# Políticas de escolha de servidores para novas conexões

from threading import Lock


# Round-Robin: percorrer os servidores em ordem circular

class RoundRobin:

    def __init__(self, servers, lock=None):
        self.servers = servers
        self.lock = lock if lock is not None else Lock()
        self._next_index = 0

    def select_server(self):
        with self.lock:
            # Faz no máximo uma volta, pulando os DOWN e os desabilitados.
            for _ in range(len(self.servers)):
                index = self._next_index % len(self.servers)
                self._next_index = (index + 1) % len(self.servers)
                server = self.servers[index]
                if server._enable == "YES" and server.status_server == "UP":
                    return (server.ip, server.port)
            return None


# Least Connections: escolher quem tem menos conexões abertas

class LeastConnections:

    def __init__(self, servers, lock=None):
        self.servers = servers
        self.lock = lock if lock is not None else Lock()

    def select_server(self):
        with self.lock:
            available = [
                server for server in self.servers
                if server._enable == "YES" and server.status_server == "UP"
            ]
            if not available:
                return None
            # Em empate, min() mantém o primeiro na ordem do cadastro.
            server = min(available, key=lambda server: server.num_connections)
            return (server.ip, server.port)
