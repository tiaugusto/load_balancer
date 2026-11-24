# Monitorar os servidores e coordenar as operações do menu.

import ipaddress
import math
import multiprocessing
import signal
import socket
import threading
import time

from management.configure_logs import configure_logs
from servers.Server import TCPServer
from servers.Table import ServerTable


monitor_logs = configure_logs("Monitor")
PROCESS_STOP_TIMEOUT = 2


def run_server_process(address, server_id, ready):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        server = TCPServer(address, server_id)
    except OSError:
        monitor_logs.exception("Servidor %s não conseguiu iniciar", server_id)
        return
    ready.set()
    try:
        server.serve_forever()
    finally:
        server.shutdown()


def probe_server(server, timeout):
    deadline = time.monotonic() + timeout
    expected = ("OK server=%s\n" % server.id).encode("ascii")
    try:
        with socket.create_connection(server.get_address(), timeout=timeout) as connection:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            connection.settimeout(remaining)
            connection.sendall(b"health\n")
            response = bytearray()
            while b"\n" not in response and len(response) < len(expected):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                connection.settimeout(remaining)
                data = connection.recv(len(expected) - len(response))
                if not data:
                    return False
                response.extend(data)
            return response == expected
    except OSError:
        return False


class Monitor:

    def __init__(self, table, interval=2, timeout=0.5, restart_delay=15,
                 max_restarts=3, startup_timeout=3, should_stop=None):
        if not all(math.isfinite(v) and v > 0 for v in [interval, timeout, startup_timeout]):
            raise ValueError("Intervalos e timeouts devem ser positivos e finitos")
        if not math.isfinite(restart_delay) or restart_delay < 0:
            raise ValueError("Espera de reinício deve ser não negativa e finita")
        if type(max_restarts) is not int or max_restarts < 0:
            raise ValueError("Limite de reinícios deve ser um inteiro não negativo")
        if any(s._enable == "YES" and not ipaddress.IPv4Address(s.ip).is_loopback
               for s in table.get_servers()):
            raise ValueError("Os processos dos servidores precisam de endereços loopback")
        self.table = table
        self.interval = interval
        self.timeout = timeout
        self.restart_delay = restart_delay
        self.max_restarts = max_restarts
        self.startup_timeout = startup_timeout
        self._stop = threading.Event()
        self.should_stop = should_stop or (lambda: False)
        self._process_lock = threading.Lock()
        # Operações do menu e verificações não podem se intercalar.
        self._control_lock = threading.RLock()
        self._processes = {}
        self._attempts = {}
        self._failed_since = {}
        self._thread = None
        self._context = multiprocessing.get_context("spawn")

    def start(self):
        """Iniciar os servidores habilitados e o laço de monitoramento."""
        if self._thread is not None:
            raise RuntimeError("O monitor já foi iniciado")
        for server in self.table.get_servers():
            if server._enable == "YES" and not self._stop.is_set() and not self.should_stop():
                self._launch(server)
        self.check_once()
        if not self._stop.is_set() and not self.should_stop():
            self._thread = threading.Thread(target=self._run)
            self._thread.start()

    def _run(self):
        while not self._stop.wait(self.interval):
            self.check_once()

    def check_once(self):
        with self._control_lock:
            self._check_once()

    def _check_once(self):
        for server in self.table.get_servers():
            if self._stop.is_set() or self.should_stop():
                break
            if server._enable != "YES":
                with self._process_lock:
                    entry = self._processes.get(server.id)
                if entry is not None and not entry[0].is_alive():
                    self.table.update_status_server(server.id, "DOWN")
                continue
            healthy = self._healthy(server)
            status = "UP" if healthy else "DOWN"
            self.table.update_status_server(server.id, status)
            if server.status_server != status:
                monitor_logs.info("Servidor %s: %s", server.id, status)
            if healthy:
                self._failed_since.pop(server.id, None)
                continue
            failed_since = self._failed_since.setdefault(server.id, time.monotonic())
            if (time.monotonic() - failed_since < self.restart_delay or
                    self._attempts.get(server.id, 0) >= self.max_restarts):
                continue
            self._stop_process(server.id)
            self._attempts[server.id] = self._attempts.get(server.id, 0) + 1
            monitor_logs.warning("Reinício servidor=%s tentativa=%s/%s", server.id,
                                 self._attempts[server.id], self.max_restarts)
            self._launch(server)
            self._failed_since[server.id] = time.monotonic()
            if self._healthy(server):
                self.table.update_status_server(server.id, "UP")
                self._failed_since.pop(server.id, None)

    def _healthy(self, server):
        with self._process_lock:
            entry = self._processes.get(server.id)
        if entry is None:
            return False
        process, ready = entry
        return process.is_alive() and ready.is_set() and probe_server(server, self.timeout)

    def _launch(self, server):
        if self._stop.is_set() or self.should_stop():
            return
        ready = self._context.Event()
        process = self._context.Process(target=run_server_process,
                                       args=(server.get_address(), server.id, ready))
        with self._process_lock:
            self._processes[server.id] = (process, ready)
        try:
            process.start()
        except OSError:
            monitor_logs.exception("Não foi possível criar o processo servidor=%s", server.id)
            return
        monitor_logs.info("Processo servidor=%s criado, PID=%s", server.id, process.pid)
        if not ready.wait(self.startup_timeout):
            monitor_logs.warning("Servidor %s não confirmou inicialização", server.id)

    def _stop_process(self, server_id):
        with self._process_lock:
            entry = self._processes.pop(server_id, None)
        if entry is None:
            return
        process, _ = entry
        if process.pid is not None:
            if process.is_alive():
                process.terminate()
            process.join(PROCESS_STOP_TIMEOUT)
            if process.is_alive():
                process.kill()
                process.join(PROCESS_STOP_TIMEOUT)
        process.close()

    def get_processes(self):
        with self._process_lock:
            return {id: process.pid for id, (process, _) in self._processes.items()
                    if process.is_alive()}

    def stop(self):
        self._stop.set()
        if self._thread is not None and self._thread.ident is not None:
            self._thread.join()
        with self._control_lock:
            for server in self.table.get_servers():
                self._stop_process(server.id)
                self.table.update_status_server(server.id, "DOWN")

    def execute(self, action, *arguments):
        """Executar uma operação administrativa na mesma tabela usada pelo LB."""
        with self._control_lock:
            if self._stop.is_set() or self.should_stop():
                raise ValueError("O sistema está encerrando")
            if action == "reload":
                self._reload_config()
                return "Cadastro recarregado"
            if action == "create":
                ip, port = arguments
                if not ipaddress.IPv4Address(ip).is_loopback:
                    raise ValueError("O servidor deve usar um endereço loopback local")
                self.table.servers_create(ip, port)
                result = "Servidor cadastrado DOWN e desabilitado"
            elif action in ("set_ip", "set_port"):
                id, value = arguments
                server = self.table.get_server(id)
                ip = value if action == "set_ip" else server.ip
                if not ipaddress.IPv4Address(ip).is_loopback:
                    raise ValueError("O servidor deve usar um endereço loopback local")
                if action == "set_ip":
                    self.table.set_ip(id, value)
                else:
                    self.table.set_port(id, value)
                self._stop_process(id)
                self._attempts.pop(id, None)
                self._failed_since.pop(id, None)
                result = "OK"
            else:
                all_servers = action.endswith("_all")
                operation = action.removesuffix("_all")
                if operation not in ("enable", "disable", "start", "stop", "delete"):
                    raise ValueError("Operação desconhecida")
                if all_servers:
                    ids = [server.id for server in self.table.get_servers()]
                else:
                    ids = [self.table.get_server(arguments[0]).id]
                if operation in ("enable", "start"):
                    for id in ids:
                        if not ipaddress.IPv4Address(self.table.get_server(id).ip).is_loopback:
                            raise ValueError("O servidor deve usar um endereço loopback local")
                if operation in ("enable", "disable"):
                    for id in ids:
                        previous = self.table.get_server(id)
                        if operation == "enable":
                            self.table._enable_server(previous)
                            if previous._enable != "YES":
                                self._attempts.pop(id, None)
                                self._failed_since.pop(id, None)
                        else:
                            self.table._unable_server(previous)
                elif operation == "start":
                    for id in ids:
                        self._start_manually(id)
                else:
                    # Bloquear novas reservas antes de aguardar o término dos processos.
                    self.table.prepare_stop(ids)
                    for id in ids:
                        self._stop_process(id)
                        self._attempts.pop(id, None)
                        self._failed_since.pop(id, None)
                        if operation == "delete":
                            self.table.destroy_server(id)
                result = "OK"
            try:
                self.table.saveTable()
            except OSError:
                monitor_logs.exception("Operação aplicada, mas o cadastro não pôde ser salvo")
                result += "; alteração em memória, mas houve falha ao salvar o cadastro"
            return result

    def _reload_config(self):
        """Conciliar o JSON com a tabela e os processos sem interromper clientes mantidos."""
        if self.table.filename is None:
            raise ValueError("A tabela não possui arquivo de configuração")
        servers = ServerTable(self.table.filename).get_servers()
        if any(s._enable == "YES" and not ipaddress.IPv4Address(s.ip).is_loopback for s in servers):
            raise ValueError("Os processos dos servidores precisam de endereços loopback")
        previous = {server.id: server for server in self.table.get_servers()}
        changed = self.table.apply_config(servers)
        for id in changed:
            self._stop_process(id)
            self._attempts.pop(id, None)
            self._failed_since.pop(id, None)
        for server in servers:
            old = previous.get(server.id)
            if old is None or server.id in changed:
                if server._enable == "YES":
                    self._launch(server)
            elif server._enable == "YES" and old._enable != "YES":
                self._attempts.pop(server.id, None)
                self._failed_since.pop(server.id, None)
        self._check_once()

    def _start_manually(self, id):
        server = self.table.get_server(id)
        if not ipaddress.IPv4Address(server.ip).is_loopback:
            raise ValueError("O servidor deve usar um endereço loopback local")
        if self._healthy(server):
            self._attempts.pop(id, None)
            self._failed_since.pop(id, None)
            self.table.update_status_server(id, "UP")
            return
        self.table.prepare_start(id)
        self._stop_process(id)
        self._attempts.pop(id, None)
        self._failed_since.pop(id, None)
        self._launch(server)
        healthy = self._healthy(server)
        self.table.update_status_server(id, "UP" if healthy else "DOWN")
        if not healthy:
            raise ValueError("Servidor não conseguiu iniciar; confira o log do monitor")
