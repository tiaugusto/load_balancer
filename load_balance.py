"""LB TCP: recebe clientes, escolhe um servidor e repassa os dados nos dois sentidos."""

import socket
import threading
from io import DEFAULT_BUFFER_SIZE

from management.configure_logs import load_balanced_logs


logger = load_balanced_logs


def close_connection(connection):
    # shutdown() acorda as threads antes de fechar o socket.
    try:
        connection.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    connection.close()


class LoadBalancer:

    def __init__(self, address, select_server, release_server=None,
                 connect_timeout=3.0, should_stop=None):
        if connect_timeout <= 0:
            raise ValueError("Timeout deve ser positivo")
        self.select_server = select_server
        self.release_server = release_server
        self.connect_timeout = connect_timeout
        # O lock protege somente o registro de sockets e threads do LB.
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self.should_stop = should_stop or (lambda: False)
        self._connections = set()
        self._workers = set()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._listener.bind(address)
            self._listener.listen()
            self._listener.settimeout(0.2)
            self.address = self._listener.getsockname()
        except BaseException:
            self._listener.close()
            raise

    # Aceitar os clientes e iniciar suas threads

    def serve_forever(self):
        logger.info("LB ouvindo em %s:%s", *self.address)
        try:
            while not self._stopped.is_set() and not self.should_stop():
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
            if self._stopped.is_set():
                close_connection(client)
                return
            client.settimeout(None)
            self._connections.add(client)
            worker = threading.Thread(target=self._handle_client, args=(client,))
            self._workers.add(worker)
            try:
                worker.start()
            except BaseException as error:
                self._workers.discard(worker)
                self._connections.discard(client)
                close_connection(client)
                if not isinstance(error, RuntimeError):
                    raise
                logger.exception("Não foi possível iniciar atendimento; conexão encerrada")

    def shutdown(self):
        with self._lock:
            self._stopped.set()
            connections = list(self._connections)
            workers = list(self._workers)
        close_connection(self._listener)
        for connection in connections:
            close_connection(connection)
        for worker in workers:
            if worker is not threading.current_thread():
                worker.join()

    # Escolher o servidor e atender a conexão

    def _handle_client(self, client):
        server_address = None
        server = None
        response_worker = None
        try:
            # Escolher já reserva a conexão; None significa indisponível.
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
            response_worker = threading.Thread(
                target=self._relay,
                args=(server, client)
            )
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
            # Desfaz a reserva também se a conexão com o servidor falhar.
            if server_address is not None and self.release_server is not None:
                try:
                    self.release_server(server_address)
                except Exception:
                    logger.exception("Falha ao liberar reserva de %s", server_address)
            with self._lock:
                self._connections.discard(client)
                self._connections.discard(server)
                self._workers.discard(threading.current_thread())

    # Repassar os bytes sem interpretar o conteúdo

    def _relay(self, source, destination):
        try:
            while not self._stopped.is_set():
                data = source.recv(DEFAULT_BUFFER_SIZE)
                if not data:
                    # Fim do envio: o outro sentido ainda pode trazer a resposta.
                    destination.shutdown(socket.SHUT_WR)
                    return
                destination.sendall(data)
        except OSError:
            if not self._stopped.is_set():
                logger.warning("Conexão interrompida durante encaminhamento")
            # Um erro precisa acordar também a thread do outro sentido.
            close_connection(source)
            close_connection(destination)
