import json
import threading
import subprocess
import sys
import time

from pathlib import Path
from .server_class import Server
from management.configure_logs import server_logs


filename_server_directory = Path(__file__).parent
filename_root_directory = Path(__file__).resolve().parent.parent

"""
 
    def __init__(self, filename="tabela_servidores.json"):
        Inicializa a tabela de servidores, configurando o caminho do arquivo
        JSON, a lista de servidores, o contador de conexões, o mecanismo de
        sincronização entre threads e o dicionário de processos ativos.
        Ao final, carrega os dados armazenados.
 
    def load(self):
        Carrega as informações dos servidores armazenadas no arquivo JSON.
        Reconstrói os objetos Server, restaura seus atributos e inicia os
        servidores que estavam registrados com status UP.
 
    def saveTable(self):
        Salva as informações atuais da tabela de servidores no arquivo JSON,
        incluindo identificadores, endereços, portas, estados, quantidade de
        conexões e disponibilidade de cada servidor.
 
    def servers_create(self, ip, port):
        Cadastra um novo servidor na tabela, validando a porta e verificando
        se o endereço IP e a porta já estão sendo utilizados por outro servidor.
        Atribui um identificador único e persiste as alterações no JSON.
 
    def destroy_all_server(self):
        Remove todos os servidores cadastrados, encerrando seus processos
        antes da remoção. A operação não é permitida enquanto existirem
        conexões ativas.
 
    def destroy_server(self, id):
        Remove um servidor específico da tabela a partir de seu identificador.
        Antes da remoção, verifica se existem conexões ativas e encerra
        o processo associado ao servidor.
 
    def _enable_server(self, server1):
        Habilita um servidor para que ele possa receber novas conexões
        encaminhadas pelo Load Balancer. Atualiza o atributo _enable
        e registra a operação nos logs.
 
    def _enable_all_servers(self):
        Habilita todos os servidores cadastrados, permitindo que sejam
        considerados disponíveis para receber novas conexões do
        Load Balancer.
 
    def _unable_server(self, server1):
        Desabilita um servidor para impedir que ele receba novas conexões
        do Load Balancer. Não encerra o processo do servidor, apenas
        modifica seu estado de habilitação.
 
    def _unable_all_servers(self):
        Desabilita todos os servidores cadastrados, impedindo que recebam
        novas conexões encaminhadas pelo Load Balancer.
 
    def _start_server(self, server):
        Inicia o processo de um servidor utilizando subprocess.Popen.
        Verifica se o processo já está em execução, registra o processo
        ativo e atualiza seu status para UP. Em caso de falha, registra
        o erro e mantém o status como DOWN.
 
    def _stop_server(self, server):
        Encerra o processo de um servidor utilizando terminate().
        Caso o processo não finalize dentro do tempo limite, utiliza kill().
        Remove o processo do controle interno e atualiza seu status para DOWN.
 
    def start_all_servers(self):
        Inicia todos os servidores cadastrados, registrando os identificadores
        daqueles cuja inicialização falhar. Retorna OK caso todos sejam
        iniciados corretamente.
 
    def stop_all_servers(self):
        Encerra os processos de todos os servidores cadastrados.
        A operação é impedida caso algum servidor possua conexões ativas.
 
    def update_status_server(self, id, status):
        Atualiza o estado operacional de um servidor específico.
        Recebe o identificador e o novo status (UP ou DOWN), realizando
        a inicialização ou o encerramento do processo correspondente.
 
    def set_ip(self, id, ip):
        Altera o endereço IP de um servidor específico.
        A alteração só é permitida quando o servidor está desligado
        (DOWN) e desabilitado (NO).
 
    def set_port(self, id, port):
        Altera a porta de comunicação de um servidor específico.
        Valida o intervalo permitido e verifica possíveis conflitos
        de endereço. A alteração só é permitida quando o servidor
        está desligado e desabilitado.
 
    def register_connection(self, id):
        Registra uma nova conexão associada a um servidor.
        Incrementa o número de conexões individuais e o total de conexões
        da tabela. O registro só ocorre se o servidor estiver UP e habilitado.
 
    def release_connection(self, id):
        Registra o encerramento de uma conexão de um servidor.
        Decrementa os contadores de conexões individuais e totais,
        desde que exista uma conexão previamente registrada.
 
    def get_servers(self):
        Retorna uma cópia da lista de servidores cadastrados,
        permitindo consultar os objetos sem expor diretamente
        a lista interna da classe.
 
    def list_servers(self):
        Exibe no terminal as informações dos servidores cadastrados,
        incluindo identificador, endereço IP, porta, status operacional,
        quantidade de conexões e estado de habilitação.
        """




class ServerTable:

    def __init__(self, filename="tabela_servidores.json"):

        self.filename = filename_server_directory / filename
        self.servers = []
        self.num_total_connections = 0
        self.lock = threading.RLock()
        self.processes = {}

        self.load()

    def load(self):

        with self.lock:

            if not self.filename.exists():
                self.saveTable()
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
                    item.get("status_server", "DOWN"),
                    item.get("_enable", "NO")
                )

                server.num_connections = 0
                self.servers.append(server)

            for server in self.servers:

                if server.status_server == "UP":
                    self._start_server(server)

            self.saveTable()

    def saveTable(self):

        data = {
            "num_total_connections": self.num_total_connections,
            "servers": [
                {
                    "id": server.id,
                    "ip": server.ip,
                    "port": server.port,
                    "status_server": server.status_server,
                    "num_connections": server.num_connections,
                    "_enable": server._enable
                }
                for server in self.servers
            ]
        }

        self.filename.parent.mkdir(parents=True, exist_ok=True)

        with open(self.filename, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)

    def servers_create(self, ip, port):

        with self.lock:

            if not isinstance(port, int) or not 1 <= port <= 65535:
                return "Porta inválida"

            server_id = max(
                (server.id for server in self.servers),
                default=-1
            ) + 1

            server_new = Server(server_id, ip, port)

            if any(server.get_address() == server_new.get_address() for server in self.servers):
                return "Servidor já cadastrado"

            self.servers.append(server_new)
            self.saveTable()

            server_logs.info(f"Servidor {server_id} foi criado!")

            return "OK"

    def destroy_all_server(self):

        with self.lock:

            if self.num_total_connections > 0:
                return "Existem conexões ativas"

            for server in self.servers:
                self._stop_server(server)

            self.servers.clear()
            self.processes.clear()
            self.num_total_connections = 0

            self.saveTable()

            server_logs.info("Todos os servidores foram destruídos")

            return "OK"

    def destroy_server(self, id):

        with self.lock:

            for server in self.servers:

                if server.id == id:

                    if server.num_connections > 0:
                        return "Servidor possui conexões ativas"

                    self._stop_server(server)
                    self.servers.remove(server)
                    self.saveTable()

                    server_logs.info(f"Servidor {id} foi destruído!")

                    return "OK"

            return "Servidor não encontrado"

    def _enable_server(self, server1):

        with self.lock:

            if server1._enable == "YES":
                server_logs.info(f"Tentativa redundante de habilitar servidor {server1.id}")
                return True

            try:
                server1._enable = "YES"
                self.saveTable()
                server_logs.info(f"Servidor {server1.id} habilitado!")
                return True

            except Exception as error:
                server1._enable = "NO"
                server_logs.error(f"Falha ao habilitar servidor {server1.id}: {error}")
                return False

    def _enable_all_servers(self):

        with self.lock:

            for server in self.servers:
                self._enable_server(server)

            self.saveTable()

            return "OK"

    def _unable_server(self, server1):

        with self.lock:

            if server1._enable == "NO":
                server_logs.info(f"Tentativa redundante de desabilitar servidor {server1.id}")
                return True

            try:
                server1._enable = "NO"
                self.saveTable()
                server_logs.info(f"Servidor {server1.id} desabilitado!")
                return True

            except Exception as error:
                server1._enable = "YES"
                server_logs.error(f"Falha ao desabilitar servidor {server1.id}: {error}")
                return False

    def _unable_all_servers(self):

        with self.lock:

            for server in self.servers:
                self._unable_server(server)

            self.saveTable()

            return "OK"

    def _start_server(self, server):

        with self.lock:
            
            process = self.processes.get(server.id)

            if process is not None and process.poll() is None:
                server.status_server = "UP"
                self.saveTable()
                return True

            try:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "servers.Server",
                        server.ip,
                        str(server.port)
                    ],
                    cwd=filename_root_directory
                )

                time.sleep(0.2)

                if process.poll() is not None:
                    server.status_server = "DOWN"
                    server_logs.error(
                        f"Falha ao iniciar servidor {server.id}. "
                        f"Código de saída: {process.returncode}"
                    )
                    self.saveTable()
                    return False

                self.processes[server.id] = process
                server.status_server = "UP"

                server_logs.info(f"Processo do servidor {server.id} iniciado!")

                self.saveTable()
                return True

            except (OSError, subprocess.SubprocessError) as error:

                server.status_server = "DOWN"

                server_logs.error(f"Erro ao iniciar servidor {server.id}: {error}")

                self.saveTable()
                return False

    def _stop_server(self, server):

        with self.lock:

            process = self.processes.get(server.id)

            if process is not None:

                if process.poll() is None:

                    process.terminate()

                    try:
                        process.wait(timeout=5)

                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()

                self.processes.pop(server.id, None)

            server.status_server = "DOWN"

            server_logs.info(f"Processo do servidor {server.id} encerrado!")

            self.saveTable()

            return True
    def start_all_servers(self):

        with self.lock:

            erros = []

            for server in self.servers:

                if not self._start_server(server):
                    erros.append(server.id)

            self.saveTable()

            if erros:
                return f"Falha ao ativar os servidores: {erros}"

            return "OK"

    def stop_all_servers(self):

        with self.lock:

            for server in self.servers:

                if server.num_connections > 0:
                    return f"Servidor {server.id} possui conexões ativas"

            for server in self.servers:
                self._stop_server(server)

                server_logs.info(f"Servidor {server.id} foi desativado!")

            self.saveTable()

            return "OK"

    def update_status_server(self, id, status):

        if status not in ("UP", "DOWN"):
            return "Status inválido! Favor inserir novamente"

        with self.lock:

            for server in self.servers:

                if server.id == id:

                    if status == "UP":

                        if not self._start_server(server):
                            return "Erro ao iniciar o servidor"

                    else:

                        if server.num_connections > 0:
                            return "O servidor ainda possui conexões ativas"

                        self._stop_server(server)

                    self.saveTable()

                    server_logs.info(f"Servidor {id} alterado para o status: {status}")

                    return "OK"

            return "Servidor não foi encontrado"

    def set_ip(self, id, ip):

        with self.lock:

            for server in self.servers:

                if server.id == id:

                    if server.status_server == "DOWN" and server._enable == "NO":

                        ip_aux    = server.ip
                        server.ip = ip

                        self.saveTable()

                        print(f"Server ID {server.id} trocou de IP. Foi de {ip_aux} para {server.ip}")

                        server_logs.info(f"Server ID {server.id} trocou de IP. Foi de {ip_aux} para {server.ip}")

                        return True

                    print(f"Erro ao trocar o IP do servidor de ID {server.id}, pois ele está habilitado. O servidor deve estar baixo e desabilitado para poder fazer troca de informações de configuração!")

                    server_logs.error(f"Erro ao trocar o IP do servidor de ID {server.id}, pois ele está habilitado.")

                    return False

            print(f"Servidor de ID {id} não encontrado!")

            server_logs.error(f"Servidor de ID {id} não encontrado!")

            return False

    def set_port(self, id, port):

        with self.lock:

            if not isinstance(port, int) or not 1 <= port <= 65535:
                return False

            for server in self.servers:

                if server.id == id:

                    if server.status_server == "DOWN" and server._enable == "NO":

                        port_aux    = server.port
                        server.port = port

                        if any(s.id != server.id and s.get_address() == server.get_address() for s in self.servers):
                            server.port = port_aux
                            return False

                        self.saveTable()

                        print(f"Server ID {server.id} trocou de porta. Foi de {port_aux} para {server.port}")

                        server_logs.info(f"Server ID {server.id} trocou de porta. Foi de {port_aux} para {server.port}")

                        return True

                    print(f"Erro ao trocar a porta do servidor de ID {server.id}, pois ele está habilitado. O servidor deve estar baixo e desabilitado para poder fazer troca de informações de configuração!")

                    server_logs.error(f"Erro ao trocar a porta do servidor de ID {server.id}, pois ele está habilitado.")

                    return False

            print(f"Servidor de ID {id} não encontrado!")

            server_logs.error(f"Servidor de ID {id} não encontrado!")

            return False

    def register_connection(self, id):

        with self.lock:

            for server in self.servers:

                if server.id == id:

                    if server.status_server != "UP" or server._enable != "YES":
                        return False

                    server.num_connections     += 1
                    self.num_total_connections += 1

                    self.saveTable()

                    return True

            return False

    def release_connection(self, id):

        with self.lock:

            for server in self.servers:

                if server.id == id:

                    if server.num_connections > 0:

                        server.num_connections     -= 1
                        self.num_total_connections -= 1

                        self.saveTable()

                        return True

                    return False

            return False

    def get_servers(self):

        with self.lock:
            return list(self.servers)

    def list_servers(self):

        print("\n--- SERVIDORES CADASTRADOS ---")

        for server in self.get_servers():

            print(
                f"ID: {server.id} | "
                f"IP: {server.ip} | "
                f"PORTA: {server.port} | "
                f"STATUS: {server.status_server} | "
                f"CONEXÕES: {server.num_connections} | "
                f"ENABLE: {server._enable}"
            )


if __name__ == "__main__":

    table = ServerTable()

    table.list_servers()

    print("\n--- TESTANDO ATIVAÇÃO ---")

    print(table.update_status_server(0, "UP"))

    table.list_servers()