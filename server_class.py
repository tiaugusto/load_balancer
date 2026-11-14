import json
import threading
from pathlib import Path

from configure_logs import server_logs


class Server:

    def __init__(self, id, ip, port, status_server="DOWN"):
        self.id = id
        self.status_server = status_server
        self.num_connections = 0
        self.ip = ip
        self.port = port

    def get_address(self):
        return (self.ip, self.port)


class ServerTable:

    def __init__(self, filename="servers.json"):
        self.filename = Path(__file__).parent / filename
        self.servers = []
        self.num_total_connections = 0
        self.lock = threading.Lock()

        self.load()

    #Carregar e salvar JSON

    def load(self):
        with self.lock:
            if not self.filename.exists():
                self._save()
                return

            with open(self.filename, "r", encoding="utf-8") as file:
                data = json.load(file)

            self.num_total_connections = 0

            self.servers = []

            for item in data.get("servers", []):
                server = Server(
                    item["id"],
                    item["ip"],
                    item["port"],
                    item["status_server"]
                )

                # Conexões antigas não existem após reiniciar o LB.
                server.num_connections = 0

                self.servers.append(server)

            self._save()

    def _save(self):
        data = {
            "num_total_connections": self.num_total_connections,
            "servers": [
                {
                    "id": server.id,
                    "ip": server.ip,
                    "port": server.port,
                    "status_server": server.status_server,
                    "num_connections": server.num_connections
                }
                for server in self.servers
            ]
        }

        with open(self.filename, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)

    # Criar e descriar os servidores

    def servers_create(self, ip, port):
        with self.lock:
            server_id = max(
                (server.id for server in self.servers),
                default=-1
            ) + 1

            server_new = Server(server_id, ip, port)

            if any(s.get_address() == server_new.get_address()
                   for s in self.servers):
                return "Servidor já cadastrado"

            self.servers.append(server_new)
            self._save()

            server_logs.info(f"Servidor {server_id} foi criado!")

            return "OK"

    def destroy_all_server(self):
        with self.lock:
            if self.num_total_connections > 0:
                return "Existem conexões ativas"

            self.servers.clear()
            self._save()

            server_logs.info("Todos os servidores foram destruídos")

            return "OK"

    # Ativar e desativar servidores

    def active_all_servers(self):
        with self.lock:
            for server in self.servers:
                server.status_server = "UP"
                server_logs.info(f"Servidor {server.id} foi ativado!")

            self._save()
            return "OK"

    def desactive_all_servers(self):
        with self.lock:
            for server in self.servers:
                server.status_server = "DOWN"
                server_logs.info(f"Servidor {server.id} foi desativado!")

            self._save()
            return "OK"

    def active_server(self, id):
        return self.update_status(id, "UP")

    def desactive_server(self, id):
        return self.update_status(id, "DOWN")

    def update_status(self, id, status):
        with self.lock:
            for server in self.servers:
                if server.id == id:
                    server.status_server = status
                    self._save()

                    server_logs.info(
                        f"Servidor {id} alterado para {status}"
                    )

                    return "OK"

            return "Servidor não encontrado"

    #Alterar informações dos servidores

    def set_port(self, id, port):
        with self.lock:
            for server in self.servers:
                if server.id == id:
                    old_port = server.port
                    server.port = port
                    self._save()

                    server_logs.info(
                        f"Porta do servidor {id} trocada de "
                        f"{old_port} para {port}!"
                    )

                    return "OK"

            return "Servidor não encontrado"

    def set_ip(self, id, ip):
        with self.lock:
            for server in self.servers:
                if server.id == id:
                    old_ip = server.ip
                    server.ip = ip
                    self._save()

                    server_logs.info(
                        f"IP do servidor {id} trocado de "
                        f"{old_ip} para {ip}!"
                    )

                    return "OK"

            return "Servidor não encontrado"

    # Controle de conexões

    def register_connection(self, id):
        with self.lock:
            for server in self.servers:
                if server.id == id:
                    if server.status_server != "UP":
                        return False

                    server.num_connections += 1
                    self.num_total_connections += 1

                    self._save()
                    return True

            return False

    def release_connection(self, id):
        with self.lock:
            for server in self.servers:
                if server.id == id:
                    if server.num_connections > 0:
                        server.num_connections -= 1
                        self.num_total_connections -= 1

                        self._save()
                        return True

                    return False

            return False

#consultar os servidores, se está tudo ok
    def get_servers(self):
        with self.lock:
            return list(self.servers)