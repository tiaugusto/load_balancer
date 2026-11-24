"""Servidor TCP concorrente: recebe dados do LB e responde com uma confirmação."""

import argparse
import ipaddress
import logging
import socket
import threading
from io import DEFAULT_BUFFER_SIZE

from management.configure_logs import server_logs


def close_connection(connection):
    # shutdown() acorda as threads que estão esperando dados.
    try:
        connection.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    connection.close()


class TCPServer:

    def __init__(self, address, server_id=0):
        if type(server_id) is not int or server_id < 0:
            raise ValueError("ID deve ser um inteiro não negativo")
        self.id = server_id
        self._reply = ("OK server=%s\n" % server_id).encode("ascii")
        self._lock = threading.Lock()
        self._stopped = threading.Event()
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

    # Aceitar conexões vindas do LB

    def serve_forever(self):
        server_logs.info("Servidor %s ouvindo em %s:%s", self.id, *self.address)
        try:
            while not self._stopped.is_set():
                try:
                    connection, _ = self._listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self._stopped.is_set():
                        break
                    raise
                self._start_connection(connection)
        finally:
            self.shutdown()

    def _start_connection(self, connection):
        with self._lock:
            if self._stopped.is_set():
                close_connection(connection)
                return
            connection.settimeout(None)
            self._connections.add(connection)
            worker = threading.Thread(target=self._handle_connection, args=(connection,))
            self._workers.add(worker)
            try:
                worker.start()
            except BaseException as error:
                self._workers.discard(worker)
                self._connections.discard(connection)
                close_connection(connection)
                if not isinstance(error, RuntimeError):
                    raise
                server_logs.exception("Não foi possível iniciar atendimento; conexão encerrada")

    def _handle_connection(self, connection):
        server_logs.info("Servidor %s abriu conexão", self.id)
        try:
            while not self._stopped.is_set():
                data = connection.recv(DEFAULT_BUFFER_SIZE)
                if not data:
                    break
                # Confirma o lote de bytes; um recv() não é uma mensagem completa.
                connection.sendall(self._reply)
                server_logs.info(
                    "Servidor %s recebeu %s bytes e respondeu OK",
                    self.id,
                    len(data)
                )
        except OSError:
            if not self._stopped.is_set():
                server_logs.warning("Servidor %s: conexão interrompida", self.id)
        finally:
            close_connection(connection)
            with self._lock:
                self._connections.discard(connection)
                self._workers.discard(threading.current_thread())
            server_logs.info("Servidor %s fechou conexão", self.id)

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


# Executar o servidor pela linha de comando

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--id", type=int, default=0)
    args = parser.parse_args()
    try:
        if not 1 <= args.port <= 65535:
            raise ValueError("Porta deve estar entre 1 e 65535")
        address = (str(ipaddress.IPv4Address(args.host)), args.port)
        server = TCPServer(address, server_id=args.id)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    server_logs.addHandler(logging.StreamHandler())
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
