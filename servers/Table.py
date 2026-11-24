# Cadastro e contadores compartilhados pelo menu, LB e monitor.

from copy import copy
import json
import os
from pathlib import Path
import tempfile
from threading import Lock

from management.configure_logs import server_logs
from management.politcs import RoundRobin
from servers.server_class import Server


class ServerTable:

    def __init__(self, filename="tabela_servidores.json", *, servers=None, policy_class=RoundRobin):
        self.filename = Path(__file__).parent / filename if filename is not None else None
        self.servers = list(servers) if servers is not None else []
        self._validate_servers(self.servers)
        self.lock = Lock()
        self.num_total_connections = 0
        self._policy = policy_class(self.servers)
        if servers is None:
            self.load()

    @staticmethod
    def _validate_servers(servers):
        if len({server.id for server in servers}) != len(servers):
            raise ValueError("IDs de servidores não podem se repetir")
        if len({server.get_address() for server in servers}) != len(servers):
            raise ValueError("Endereços de servidores não podem se repetir")

    def load(self):
        """Ler o cadastro; saúde e contadores serão definidos pela execução atual."""
        if self.filename is None:
            raise ValueError("A tabela não possui arquivo de configuração")
        data = json.loads(self.filename.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not isinstance(data.get("servers"), list):
            raise ValueError("O JSON deve conter uma lista servers")
        servers = []
        for record in data["servers"]:
            if not isinstance(record, dict) or not {"id", "ip", "port"} <= record.keys():
                raise ValueError("Cada servidor precisa de id, ip e port")
            if "enable" in record:
                raise ValueError("Use o campo _enable com YES/NO")
            servers.append(Server(record["id"], record["ip"], record["port"],
                                  _enable=record.get("_enable", "NO")))
        self._validate_servers(servers)
        self.apply_config(servers)

    def _find(self, id):
        server = next((s for s in self.servers if s.id == id), None)
        if server is None:
            raise ValueError("Servidor não encontrado")
        return server

    def get_server(self, id):
        with self.lock:
            return copy(self._find(id))

    def update_status_server(self, id, status):
        if status not in ("UP", "DOWN"):
            raise ValueError("Status deve ser UP ou DOWN")
        with self.lock:
            server = self._find(id)
            if server.status_server != status:
                server.status_server = status
                server_logs.info("Servidor %s marcado %s", id, status)
            return "OK"

    def select_server(self):
        """Escolher e reservar o destino sob o mesmo lock, inclusive no Least Connections."""
        with self.lock:
            address = self._policy.select_server()
            if address is not None:
                server = next(s for s in self.servers if s.get_address() == address)
                server.num_connections += 1
                self.num_total_connections += 1
            return address

    def release_server(self, address):
        with self.lock:
            server = next((s for s in self.servers if s.get_address() == address), None)
            if server is None or not server.num_connections:
                raise ValueError("Servidor não possui conexão para liberar")
            server.num_connections -= 1
            self.num_total_connections -= 1

    def servers_create(self, ip, port):
        with self.lock:
            id = max((s.id for s in self.servers), default=-1) + 1
            server = Server(id, ip, port)
            if any(s.get_address() == server.get_address() for s in self.servers):
                raise ValueError("Endereço já cadastrado")
            self.servers.append(server)
            return "OK"

    def _enable_server(self, server1):
        with self.lock:
            server = self._find(server1.id)
            if server._enable != "YES":
                server.status_server = "DOWN"
            server._enable = "YES"
            return True

    def _unable_server(self, server1):
        with self.lock:
            self._find(server1.id)._enable = "NO"
            return True

    def prepare_stop(self, ids):
        """Recusar servidores com clientes ativos e bloquear novas escolhas antes da parada."""
        with self.lock:
            servers = [self._find(id) for id in ids]
            if any(s.num_connections for s in servers):
                raise ValueError("Existem conexões ativas; desabilite e aguarde a liberação")
            for server in servers:
                server._enable = "NO"
                server.status_server = "DOWN"

    def prepare_start(self, id):
        with self.lock:
            server = self._find(id)
            if server.num_connections:
                raise ValueError("Existem conexões ativas; aguarde a liberação antes de iniciar")
            server.status_server = "DOWN"

    def destroy_server(self, id):
        with self.lock:
            server = self._find(id)
            if server.num_connections or server._enable != "NO" or server.status_server != "DOWN":
                raise ValueError("Pare e desabilite o servidor antes de removê-lo")
            self.servers.remove(server)
            return "OK"

    def set_ip(self, id, ip):
        self._set_address(id, ip=ip)
        return True

    def set_port(self, id, port):
        self._set_address(id, port=port)
        return True

    def _set_address(self, id, ip=None, port=None):
        with self.lock:
            server = self._find(id)
            if server._enable != "NO" or server.status_server != "DOWN" or server.num_connections:
                raise ValueError("O servidor deve estar DOWN, desabilitado e sem conexões")
            candidate = Server(id, server.ip if ip is None else ip,
                               server.port if port is None else port)
            if any(s.id != id and s.get_address() == candidate.get_address() for s in self.servers):
                raise ValueError("Endereço já cadastrado")
            server.ip, server.port = candidate.get_address()

    def get_servers(self):
        """Retornar cópias para que a consulta não altere os registros compartilhados."""
        with self.lock:
            return [copy(server) for server in self.servers]

    def apply_config(self, servers):
        """Preservar registros em uso e retornar os IDs removidos ou com novo endereço."""
        with self.lock:
            current = {server.id: server for server in self.servers}
            incoming = {server.id: server for server in servers}
            changed = []
            for server in self.servers:
                replacement = incoming.get(server.id)
                if replacement is None or server.get_address() != replacement.get_address():
                    changed.append(server.id)
            if any(current[server_id].num_connections for server_id in changed):
                raise ValueError("Existem conexões ativas nos servidores removidos ou alterados; "
                                 "desabilite e aguarde a liberação antes do reload")
            updated = []
            for server in servers:
                previous = current.get(server.id)
                if previous is not None and previous.get_address() == server.get_address():
                    if server._enable == "YES" and previous._enable != "YES":
                        previous.status_server = "DOWN"
                    previous._enable = server._enable
                    server = previous
                updated.append(server)
            # Manter os registros em uso e a lista consultada pela política.
            self.servers[:] = updated
            return changed

    def snapshot(self):
        """Copiar cadastro e contadores juntos para uma consulta consistente no menu."""
        with self.lock:
            return {
                "num_total_connections": self.num_total_connections,
                "servers": [
                    {
                        "id": server.id,
                        "ip": server.ip,
                        "port": server.port,
                        "status_server": server.status_server,
                        "_enable": server._enable,
                        "num_connections": server.num_connections
                    }
                    for server in self.servers
                ]
            }

    def list_servers(self):
        state = self.snapshot()
        print("Total de conexões:", state["num_total_connections"])
        for server in state["servers"]:
            print(
                f"ID: {server['id']} | IP: {server['ip']} | PORTA: {server['port']} | "
                f"STATUS: {server['status_server']} | CONEXÕES: {server['num_connections']} | "
                f"ENABLE: {server['_enable']}"
            )

    def saveTable(self):
        """Salvar o cadastro por substituição de arquivo temporário, fora do lock da tabela."""
        if self.filename is None:
            return
        data = self.snapshot()
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                             dir=self.filename.parent, delete=False) as output:
                temporary = Path(output.name)
                json.dump(data, output, indent=4)
                output.write("\n")
            os.replace(temporary, self.filename)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
