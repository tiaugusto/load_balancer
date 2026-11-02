from server_class import Server


def test_servers_create():
    print("\n--- Teste: servers_create ---")

    servers = Server.servers_create(3)

    assert len(servers) == 3
    assert servers[0].id == 0
    assert servers[1].id == 1
    assert servers[2].id == 2
    assert all(server.status_server == "DOWN" for server in servers)

    print("OK: três servidores criados e inicialmente DOWN.")


def test_destroy_all_server():
    print("\n--- Teste: destroy_all_server ---")

    servers = Server.servers_create(3)
    Server.destroy_all_server(servers)

    assert len(servers) == 0

    print("OK: todos os servidores foram removidos.")


def test_active_all_servers():
    print("\n--- Teste: active_all_servers ---")

    servers = Server.servers_create(3)
    result = Server.active_all_servers(servers)

    assert result == "OK"
    assert all(server.status_server == "UP" for server in servers)

    print("OK: todos os servidores foram ativados.")


def test_desactive_all_servers():
    print("\n--- Teste: desactive_all_servers ---")

    servers = Server.servers_create(3)
    Server.active_all_servers(servers)

    result = Server.desactive_all_servers(servers)

    assert result == "OK"
    assert all(server.status_server == "DOWN" for server in servers)

    print("OK: todos os servidores foram desativados.")


def test_active_server():
    print("\n--- Teste: active_server ---")

    servers = Server.servers_create(3)

    result = Server.active_server(servers, 1)

    assert result == "OK"
    assert servers[0].status_server == "DOWN"
    assert servers[1].status_server == "UP"
    assert servers[2].status_server == "DOWN"

    print("OK: somente o servidor 1 foi ativado.")


def test_desactive_server():
    print("\n--- Teste: desactive_server ---")

    servers = Server.servers_create(3)
    Server.active_all_servers(servers)

    result = Server.desactive_server(servers, 1)

    assert result == "OK"
    assert servers[0].status_server == "UP"
    assert servers[1].status_server == "DOWN"
    assert servers[2].status_server == "UP"

    print("OK: somente o servidor 1 foi desativado.")


def test_server_not_found():
    print("\n--- Teste: servidor inexistente ---")

    servers = Server.servers_create(3)

    result_active = Server.active_server(servers, 99)
    result_desactive = Server.desactive_server(servers, 99)

    assert result_active == "Servidor não encontrado"
    assert result_desactive == "Servidor não encontrado"

    print("OK: operações com servidor inexistente foram tratadas.")


def run_tests():
    tests = [
        test_servers_create,
        test_destroy_all_server,
        test_active_all_servers,
        test_desactive_all_servers,
        test_active_server,
        test_desactive_server,
        test_server_not_found,
    ]

    for test in tests:
        try:
            test()
        except AssertionError:
            print(f"FALHOU: {test.__name__}")
            raise

    print("\nTodos os testes passaram!")


if __name__ == "__main__":
    run_tests()