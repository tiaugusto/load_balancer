from management.configure_logs import managenment_logs
from servers.Table import ServerTable


table = ServerTable()

def descobrir_server(id_server):
    list_server = table.get_servers()

    for server in list_server:
        if server.id == id_server:
            return server

    return None


def respostas(resposta):
    match resposta:
        case "1":
            table.list_servers()
            managenment_logs.info("Lista de servidores apresentada.")

        case "2":
            resposta = menu_habilitar_desabilitar()
            return resposta

        case "3":
            resposta = menu_ativar_desativar()
            return resposta

        case "4":
            resposta = menu_adicionar_deletar()
            return resposta

        case "5":
            print("\nGabriel Oliveira de Matos - GRR20240011")
            print("\nThiago Augusto - GRRXXXXXXXX\n")
            managenment_logs.info("Informações dos autores/Alunos apresentadas.")

        case "6":
            resposta = menu_adicionar_deletar()
            return resposta

        case "13":
            resposta = menu_configuracoes()
            return resposta


        case "26":
            id = input("Digite o ID do servidor: ").strip()
            try:
                server_del = descobrir_server(id)

                if server_del is not None:
                    resultado = table._enable_server(server_del)
                    print(resultado)
                    managenment_logs.info(f"Habilitação de servidor: {resultado}")
                else:
                    print("Servidor não encontrado!")
                    managenment_logs.warning(f"Servidor {id} não encontrado para habilitação.")
            except ValueError:
                print("ID inválido!")
                managenment_logs.warning("Tentativa de habilitação do servidor falhou.")

        case "27":
            id = input("Digite o ID do servidor: ").strip()
            try:
                server_del = descobrir_server(id)                

                if server_del is not None:
                    resultado = table._unable_server(server_del)
                    print(resultado)
                    managenment_logs.info(f"Desabilitação de servidor: {resultado}")
                else:
                    print("Servidor não encontrado!")
                    managenment_logs.warning(f"Servidor {id} não encontrado para desabilitação.")
            except ValueError:
                print("ID inválido!")
                managenment_logs.warning("Tentativa de desabilitação do servidor falhou.")

        case "28":
            try:
                resultado = table._enable_all_servers()
                print(resultado)
                managenment_logs.info(f"Habilitação de todos os servidores: {resultado}")
            except ValueError:
                print("Tentativa de habilitação de todos os servidores falhou.")
                managenment_logs.warning("Tentativa de habilitação de todos os servidores falhou.")

        case "29":
            try:
                resultado = table._unable_all_servers()
                print(resultado)
                managenment_logs.info(f"Desabilitação de todos os servidores: {resultado}")
            except ValueError:
                print("Tentativa de desabilitação de todos os servidores falhou.")
                managenment_logs.warning("Tentativa de desabilitação de todos os servidores falhou.")

        case "46":
            id = input("Digite o ID do servidor: ").strip()
            try:
                server_del = descobrir_server(id)
                if server_del is not None:
                    resultado = table._start_server(server_del)
                    print(resultado)
                    managenment_logs.info(f"Ativação de servidor: {resultado}")
                else:
                    print("Servidor não encontrado!")
                    managenment_logs.warning(f"Servidor {id} não encontrado para ativação.")
            except ValueError:
                print("ID inválido!")
                managenment_logs.warning("Tentativa de ativação do servidor falhou.")

        case "47":
            id = input("Digite o ID do servidor: ").strip()
            try:
                server_del = descobrir_server(id)
                if server_del is not None:
                    resultado = table._stop_server(server_del)
                    print(resultado)
                    managenment_logs.info(f"Desativação de servidor: {resultado}")
                else:
                    print("Servidor não encontrado!")
                    managenment_logs.warning(f"Servidor {id} não encontrado para desativação.")
            except ValueError:
                print("ID inválido!")
                managenment_logs.warning("Tentativa de desativação do servidor falhou.")

        case "48":
            try:
                resultado = table.start_all_servers()
                print(resultado)
                managenment_logs.info(f"Ativação de todos os servidores: {resultado}")
            except ValueError:
                print("Tentativa de ativação de todos os servidores falhou.")
                managenment_logs.warning("Tentativa de ativação de todos os servidores falhou.")

        case "49":
            try:
                resultado = table.stop_all_servers()
                print(resultado)
                managenment_logs.info(f"Desativação de todos os servidores: {resultado}")
            except ValueError:
                print("Tentativa de desativação de todos os servidores falhou.")
                managenment_logs.warning("Tentativa de desativação de todos os servidores falhou.")

        case "66":
            ip = input("Digite o IP do servidor: ").strip()
            try:
                port = int(input("Digite a porta do servidor (se for localhost -> 49152 - 65535): "))
                resultado = table.servers_create(ip, port)
                print(resultado)
                managenment_logs.info(f"Criação de servidor: {resultado}")
            except ValueError:
                print("Porta inválida!")
                managenment_logs.warning("Tentativa de cadastro com porta inválida.")

        case "67":
            try:
                server_id = int(input("Digite o ID do servidor: "))
                resultado = table.destroy_server(server_id)
                print(resultado)
                managenment_logs.info(f"Exclusão do servidor {server_id}: {resultado}")
            except ValueError:
                print("ID inválido!")
                managenment_logs.warning("Tentativa de exclusão com ID inválido.")

        case "68":
            try:
                resultado = table.destroy_all_server()
                print(resultado)
                managenment_logs.info("Todos os servidores possíveis foram deletados!")
            except ValueError:
                managenment_logs.warning("Tentativa de exclusão de lista de servidores deu erro.")


        case "86":
            id = input("Digite o ID do servidor: ").strip()
            try:
                server_del = descobrir_server(id)
                if server_del is not None:
                    port = input("Digite a nova porta do servidor: ").strip()
                    resultado = table.set_port(id, port)
                    print(resultado)
                    managenment_logs.info(f"Troca de porta do servidor: {resultado}")
                else:
                    print("Servidor não encontrado!")
                    managenment_logs.warning(f"Servidor {id} não encontrado para troca de porta.")
            except ValueError:
                print("ID inválido!")
                managenment_logs.warning("Tentativa de troca de porta do servidor falhou.")
      
        case "87":
            id = input("Digite o ID do servidor: ").strip()
            try:
                server_del = descobrir_server(id)
                if server_del is not None:
                    ip = input("Digite o novo IP do servidor: ").strip()
                    resultado = table.set_port(id, ip)
                    print(resultado)
                    managenment_logs.info(f"Troca de ip do servidor: {resultado}")
                else:
                    print("Servidor não encontrado!")
                    managenment_logs.warning(f"Servidor {id} não encontrado para troca de ip.")
            except ValueError:
                print("ID inválido!")
                managenment_logs.warning("Tentativa de troca de ip do servidor falhou.")

        case "14":
            return "14"

        case "15":
            try:
                resultado = table.stop_all_servers()
                print(resultado)

                if resultado == "OK":
                    print("\nScript finalizado!\n")
                    managenment_logs.info("Script finalizado.")
                    return "15"

                else:
                    print("\nNão foi possível encerrar o programa.")
                    managenment_logs.warning("Tentativa de encerramento impedida.")
                    return "continuar"

            except ValueError:
                print("Problema ao encerrar o programa.")
                managenment_logs.warning("Tentativa de encerramento falhou.")
                return "continuar"

        case _:
            print("Valor não reconhecido, tente novamente!")
            managenment_logs.warning(f"Opção inválida selecionada: {resposta}")


def header_padrao():
    print("\n\n-----------------------------------\n")
    print("------- Load Balanced Menu -------")
    print("-----------------------------------\n")

    print("Menu de operações, escolha:\n")
    print("1 - Ver lista de servidores")
    print("2 - Habilitar/Desabilitar Servidores")
    print("3 - Ativar/Desativar Servidores")
    print("4 - Adicionar/Deletar Servidores")
    print("5 - Informações dos alunos\n")
    


def menu_habilitar_desabilitar():
    header_padrao()
    print("6 - Habilitar servidor específico")
    print("7 - Desabilitar servidor específico")
    print("8 - Habilitar TODOS os servidores")
    print("9 - Desabilitar TODOS os servidores")
    print("13 - Configurações\n")
    print("14 - Voltar")
    print("15 - Sair")
    resposta = input("\nResposta: ").strip()
    if resposta not in ("14", "15","13"):
        try:
            resposta = str(int(resposta) + 20)
        except ValueError:
            return "opcao_invalida"
    managenment_logs.info(f"Escolhida opção {resposta}")
    return resposta


def menu_ativar_desativar():
    header_padrao()
    print("6 - Ativar servidor específico")
    print("7 - Desativar servidor específico")
    print("8 - Ativar todos os servidores")
    print("9 - Desativar todos os servidores")
    print("13 - Configurações\n")
    print("14 - Voltar")
    print("15 - Sair")
    resposta = input("\nResposta: ").strip()
    if resposta not in ("14", "15", "13"):
        try:
            resposta = str(int(resposta) + 40)
        except ValueError:
            return "opcao_invalida"
    managenment_logs.info(f"Escolhida opção {resposta}")
    return resposta


def menu_adicionar_deletar():
    header_padrao()
    print("6 - Adicionar servidor")
    print("7 - Deletar servidor específico")
    print("8 - Deletar todos os servidores")
    print("13 - Configurações\n")
    print("14 - Voltar")
    print("15 - Sair")
    resposta = input("\nResposta: ").strip()
    if resposta not in ("14", "15", "13"):
        try:
            resposta = str(int(resposta) + 60)
        except ValueError:
            return "opcao_invalida"
    managenment_logs.info(f"Escolhida opção {resposta}")
    return resposta

def menu_configuracoes():
    header_padrao()
    print("6 - Mudar porta")
    print("7 - mudar IP")
    print("14 - Voltar")
    print("15 - Sair")
    resposta = input("\nResposta: ").strip()
    if resposta not in ("14", "15","13"):
        try:
            resposta = str(int(resposta) + 80)
        except ValueError:
            return "opcao_invalida"
    managenment_logs.info(f"Escolhida opção {resposta}")
    return resposta

resposta = ""

try:
    while resposta != "15":
        header_padrao()
        print("13 - Configurações\n")
        print("15 - Sair")
        resposta = input("\nResposta: ").strip()
        managenment_logs.info(f"Escolhida opção {resposta}")

        while resposta not in ("14", "15", "continuar", None):
            resultado = respostas(resposta)
            if resultado is not None:
                resposta = resultado
            else:
                resposta = "14"

except KeyboardInterrupt:
    print("\n\nMenu finalizado! Favor verificar novamente se os processos estão todos baixos!\n\n")
    managenment_logs.warning("O menu foi finalizado por meio do CTRL + C. Sem garantia de que todos os processos foram finalizados formalmente!")