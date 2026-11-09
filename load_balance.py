"""LB TCP: recebe clientes, escolhe um servidor e repassa os dados nos dois sentidos."""

import argparse
import ipaddress
import logging
import socket
import threading
from types import SimpleNamespace

from politcs import RoundRobin


logger = logging.getLogger("Load_balance")
BUFFER_SIZE = 65536


def close_connection(connection):
    """Desbloqueia leituras pendentes e fecha o socket, mesmo se já estiver fechado."""
    try:
        connection.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    connection.close()


class LoadBalancer:
    """Uma conexão de cliente permanece associada ao mesmo servidor.

    select_server() retorna (IP, porta), ou None se não houver servidor disponível.
    release_server(endereco), quando fornecido, desfaz a reserva feita pela seleção,
    inclusive quando a conexão ao servidor falha. Ambos podem rodar em paralelo;
    cabe à tabela proteger seleção/reserva e liberação com seu próprio lock.
    """

    def __init__(self, address, select_server, release_server=None,
                 connect_timeout=3.0, max_connections=128):
        if connect_timeout <= 0 or max_connections <= 0:
            raise ValueError("Timeout e limite de conexões devem ser positivos")
        self.select_server = select_server
        self.release_server = release_server
        self.connect_timeout = connect_timeout
        self._max_connections = max_connections
        # O lock protege somente o registro de sockets e threads do LB.
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._connections = set()
        self._workers = set()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._listener.bind(address)
            self._listener.listen(max_connections)
            self._listener.settimeout(0.2)
            self.address = self._listener.getsockname()
        except BaseException:
            self._listener.close()
            raise

    def serve_forever(self):
        """Aceita clientes; o atendimento acontece em threads separadas."""
        logger.info("LB ouvindo em %s:%s", *self.address)
        try:
            while not self._stopped.is_set():
                try:
                    client, _ = self._listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self._stopped.is_set():
                        break
                    raise
                self._start_client(client)
        finally:
            self.shutdown()

    def _start_client(self, client):
        with self._lock:
            if self._stopped.is_set() or len(self._workers) >= self._max_connections:
                close_connection(client)
                return
            client.settimeout(None)
            self._connections.add(client)
            worker = threading.Thread(target=self._handle_client, args=(client,))
            self._workers.add(worker)
            try:
                worker.start()
            except BaseException:
                self._workers.discard(worker)
                self._connections.discard(client)
                close_connection(client)
                raise

    def shutdown(self):
        """Interrompe accept/relay e espera a liberação das conexões ativas."""
        with self._lock:
            self._stopped.set()
            connections = list(self._connections)
            workers = list(self._workers)
        self._listener.close()
        for connection in connections:
            close_connection(connection)
        for worker in workers:
            if worker is not threading.current_thread():
                worker.join()

    def _handle_client(self, client):
        """Escolhe um destino e mantém o par cliente/servidor até o encerramento."""
        server_address = None
        server = None
        response_worker = None
        try:
            server_address = self.select_server()
            if server_address is None:
                logger.warning("Nenhum servidor disponível; conexão encerrada")
                return
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            with self._lock:
                if self._stopped.is_set():
                    return
                self._connections.add(server)
            server.settimeout(self.connect_timeout)
            server.connect(server_address)
            server.settimeout(None)
            logger.info("Conexão encaminhada para %s:%s", *server_address)
            # Esta thread leva cliente -> servidor; a outra traz servidor -> cliente.
            response_worker = threading.Thread(target=self._relay,
                                               args=(server, client))
            response_worker.start()
            self._relay(client, server)
            response_worker.join()
        except Exception:
            logger.exception("Falha ao atender conexão; destino=%s", server_address)
        finally:
            close_connection(client)
            if server is not None:
                close_connection(server)
            if response_worker is not None and response_worker.ident is not None:
                response_worker.join()
            if server_address is not None and self.release_server is not None:
                try:
                    self.release_server(server_address)
                except Exception:
                    logger.exception("Falha ao liberar reserva de %s", server_address)
            with self._lock:
                self._connections.discard(client)
                self._connections.discard(server)
                self._workers.discard(threading.current_thread())

    def _relay(self, source, destination):
        """Copia bytes em um sentido, sem interpretar o conteúdo das mensagens."""
        try:
            while not self._stopped.is_set():
                data = source.recv(BUFFER_SIZE)
                if not data:
                    # EOF em um sentido permite que a resposta ainda seja lida
                    # no outro (por exemplo, após client.shutdown(SHUT_WR)).
                    destination.shutdown(socket.SHUT_WR)
                    return
                destination.sendall(data)
        except OSError:
            if not self._stopped.is_set():
                logger.warning("Conexão interrompida durante encaminhamento")
            # Um erro precisa acordar também a thread do outro sentido.
            close_connection(source)
            close_connection(destination)


def ipv4(value):
    try:
        return str(ipaddress.IPv4Address(value))
    except ipaddress.AddressValueError as error:
        raise argparse.ArgumentTypeError("Informe um endereço IPv4 válido") from error


def port(value):
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Porta deve ser um inteiro") from error
    if not 1 <= number <= 65535:
        raise argparse.ArgumentTypeError("Porta deve estar entre 1 e 65535")
    return number


def server_address(value):
    ip_text, separator, port_text = value.partition(":")
    if not separator:
        raise argparse.ArgumentTypeError("Use IP:PORTA, por exemplo 127.0.0.1:9001")
    return (ipv4(ip_text), port(port_text))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", type=ipv4, default="127.0.0.1")
    parser.add_argument("--port", type=port, default=8000)
    parser.add_argument("--policy", choices=["round-robin"], default="round-robin")
    parser.add_argument("--server", type=server_address, action="append",
                        help="IP:PORTA de um servidor; repita para adicionar outros")
    parser.add_argument("--server-ip", type=ipv4)
    parser.add_argument("--server-port", type=port)
    args = parser.parse_args()
    if args.server:
        if args.server_ip is not None or args.server_port is not None:
            parser.error("Use --server ou o par --server-ip/--server-port")
        addresses = args.server
    elif args.server_ip is not None and args.server_port is not None:
        addresses = [(args.server_ip, args.server_port)]
    else:
        parser.error("Informe --server IP:PORTA ou --server-ip e --server-port")
    if len(set(addresses)) != len(addresses):
        parser.error("Não repita o mesmo IP:PORTA na lista de servidores")

    # Configuração provisória da CLI, até a integração com a tabela da #5.
    servers = [SimpleNamespace(ip=ip, port=number, status_server="UP")
               for ip, number in addresses]
    policy = RoundRobin(servers)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")
    logger.info("Política selecionada: %s", args.policy)
    balancer = LoadBalancer((args.host, args.port), policy.select_server)
    try:
        balancer.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
