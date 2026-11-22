#Arquivo responsavel por criar o processo do servidor, vai ser ele quem vai ser chamado para 
#quando precisar subir um server
#python3 -m servers.Server 8001


import socket
import threading
from   management.configure_logs import server_logs


def start_server(ip, port):
    #Processo básico de criação do servidor com atrelação a um socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind((ip, port))
    server_sock.listen()
    print(f"Server Escutando em {ip} e {port}")
    server_logs.info(f"Server Escutando em {ip} e {port}")

    try: 
        while True:
            threading.Event().wait()

    except KeyboardInterrupt:
        print(f"Encerrando servidor {ip}:{port}")
        server_logs.info(f"Servidor {ip}:{port} encerrado")

    finally:
        server_sock.close()
                

if __name__ == "__main__":
    import sys

    ip = sys.argv[1]
    port = int(sys.argv[2])

    start_server(ip, port)