# Informações de um servidor; o processo TCP fica em servers/Server.py.

import ipaddress


class Server:

    def __init__(self, id, ip, port, status_server="DOWN", _enable="NO"):
        if type(id) is not int or id < 0:
            raise ValueError("ID deve ser um inteiro não negativo")
        if not isinstance(ip, str):
            raise ValueError("IP deve ser um endereço IPv4 em texto")
        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError("Porta deve estar entre 1 e 65535")
        if status_server not in ("UP", "DOWN") or _enable not in ("YES", "NO"):
            raise ValueError("Status deve ser UP/DOWN e _enable deve ser YES/NO")
        self.id = id
        self.ip = str(ipaddress.IPv4Address(ip))
        self.port = port
        self.status_server = status_server
        self._enable = _enable
        self.num_connections = 0

    def get_address(self):
        return (self.ip, self.port)
