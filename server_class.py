from configure_logs import server_logs

####s seguintes informações devem estar presentes:
####s• Número total de conexões (mantidas pelo LB);
####s• Quantidade de conexões de cada servidor;
####s• Endereço IP e Porta de cada servidor;
####s• Status de cada servidor (UP/DOWN);

class Server:
    #rever essas definições depois
    def __init__(self, id, status_server="DOWN", ip_inserir, port):
        self.id = id
        self.status_server = status_server
        self.num_total_connections =0
        self.qtde_connections =0
        self.ip = ip_inserir
        self.port = port

#Criar servers 

    @staticmethod
    def servers_create(number):
        server_list = []

        for qtde in range(number):
            server_new = Server(qtde, "DOWN")
            server_list.append(server_new)
            server_logs.info(f"Servidor {server_new.id} foi criado!")

        return server_list

    @staticmethod
    def destroy_all_server(server_list):
        server_list.clear()
        server_logs.info("Todos os servidores foram destruídos")

#Ativar e desativar servers

    @staticmethod
    def active_all_servers(server_list):
        for server in server_list:
            server.status_server = "UP"
            server_logs.info(f"Servidor {server.id} foi ativado!")

        return "OK"

    @staticmethod
    def desactive_all_servers(server_list):
        for server in server_list:
            server.status_server = "DOWN"
            server_logs.info(f"Servidor {server.id} foi desativado!")


        return "OK"

    @staticmethod
    def active_server(server_list, id):
        for server in server_list:
            if server.id == id:
                server.status_server = "UP"
                server_logs.info(f"Servidor {server.id} foi ativado!")


                return "OK"


        return "Servidor não encontrado"

    @staticmethod
    def desactive_server(server_list, id):
        for server in server_list:
            if server.id == id:
                server.status_server = "DOWN"
                server_logs.info(f"Servidor {server.id} foi desativado!")

                return "OK"

        return "Servidor não encontrado"

#trocar informações dos servers


    def set_port(server_list, id, port):
        for server in server_list:
            if server.id == id:
                save_port = server.port
                server.port = port
                server_logs.info(f"Porta do servidor {server.id} trocada de {save_port} para {server.port}!")

                return "OK"

        return "Servidor não encontrado"


    def set_port(server_list, id, ip):
        for server in server_list:
            if server.id == id:
                save_ip = server.ip
                server.ip = ip
                server_logs.info(f"IP do servidor {server.id} trocada de {save_ip} para {server.ip}!")

                return "OK"

        return "Servidor não encontrado"




