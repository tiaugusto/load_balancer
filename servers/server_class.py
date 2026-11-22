#Classe de Servidor com as informações básicas do servidor

class Server:

    def __init__(self, id, ip, port, status_server="DOWN", _enable = "NO"):
        self.id              = id
        self.status_server   = status_server
        self.num_connections = 0
        self.ip              = ip
        self.port            = port
        self._enable         = _enable

    def get_address(self):
        return (self.ip, self.port)
