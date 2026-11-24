"""Menu administrativo integrado ao LB e ao monitoramento."""

import os
import select
import sys

from management.configure_logs import managenment_logs


def read_line(prompt, should_stop):
    print(prompt, end="", flush=True)
    data = bytearray()
    # Ler sem bloquear a parada e sem antecipar outras linhas do stdin.
    while not should_stop():
        if not select.select([sys.stdin], [], [], 0.2)[0]:
            continue
        char = os.read(sys.stdin.fileno(), 1)
        if not char:
            raise EOFError
        if char == b"\n":
            return data.decode(sys.stdin.encoding or "utf-8").strip()
        data.extend(char)
    raise EOFError


def header_padrao():
    print("\n--- Load Balanced Menu ---\n"
          "1 - Ver lista de servidores\n"
          "2 - Habilitar/Desabilitar servidores\n"
          "3 - Ativar/Desativar servidores\n"
          "4 - Adicionar/Deletar servidores\n"
          "5 - Informações da equipe\n"
          "13 - Configurações\n15 - Sair", flush=True)


def respostas(resposta, table, monitor, ask):
    match resposta:
        case "1":
            table.list_servers()
            return True
        case "5":
            print("Tiago Augusto Fiatte Prestes - GRR20232334\n"
                  "Gabriel Oliveira de Matos - GRR20240011\n"
                  "Identificação restante da equipe: revisar no relatório.")
            return True
        case "15":
            return False
        case "2":
            print("6 - Habilitar servidor\n7 - Desabilitar servidor\n"
                  "8 - Habilitar todos\n9 - Desabilitar todos")
        case "3":
            print("6 - Ativar servidor\n7 - Desativar servidor\n"
                  "8 - Ativar todos\n9 - Desativar todos")
        case "4":
            print("6 - Adicionar servidor\n7 - Deletar servidor\n8 - Deletar todos")
        case "13":
            print("6 - Mudar porta\n7 - Mudar IP\n8 - Recarregar tabela do JSON")
        case _:
            print("Opção inválida")
            return True
    if resposta != "13":
        print("13 - Configurações")
    print("14 - Voltar\n15 - Sair", flush=True)
    selected = ask("Resposta: ")
    if selected == "15":
        return False
    if selected == "14":
        return True
    if selected == "13" and resposta != "13":
        return respostas("13", table, monitor, ask)
    match (resposta, selected):
        case ("2", "6"):
            action = "enable"
        case ("2", "7"):
            action = "disable"
        case ("2", "8"):
            action = "enable_all"
        case ("2", "9"):
            action = "disable_all"
        case ("3", "6"):
            action = "start"
        case ("3", "7"):
            action = "stop"
        case ("3", "8"):
            action = "start_all"
        case ("3", "9"):
            action = "stop_all"
        case ("4", "6"):
            action = "create"
        case ("4", "7"):
            action = "delete"
        case ("4", "8"):
            action = "delete_all"
        case ("13", "6"):
            action = "set_port"
        case ("13", "7"):
            action = "set_ip"
        case ("13", "8"):
            action = "reload"
        case _:
            print("Opção inválida")
            return True
    try:
        arguments = []
        if action == "create":
            arguments = [ask("IP: "), int(ask("Porta: "))]
        elif action != "reload" and not action.endswith("_all"):
            arguments = [int(ask("ID do servidor: "))]
            if action == "set_port":
                arguments.append(int(ask("Nova porta: ")))
            elif action == "set_ip":
                arguments.append(ask("Novo IP: "))
        result = monitor.execute(action, *arguments)
        print(result)
        managenment_logs.info("Operação %s: %s", action, result)
    except (ValueError, OSError) as error:
        print("Operação recusada:", error)
        managenment_logs.warning("Operação recusada: %s", error)
    return True


def run_menu(table, monitor, should_stop):
    def ask(prompt):
        return read_line(prompt, should_stop)

    try:
        while not should_stop():
            header_padrao()
            if not respostas(ask("Resposta: "), table, monitor, ask):
                return
    except EOFError:
        return


if __name__ == "__main__":
    from run_system import main
    raise SystemExit(main(default_menu=True))
