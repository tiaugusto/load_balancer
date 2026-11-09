# Round-Robin — issue #6

O Round-Robin distribui novas conexões em sequência circular: A, B, C, A, B, C.
Servidores `DOWN` são pulados. Se B cair, a sequência passa a ser A, C, A, C.
A política não transfere uma conexão existente de um servidor para outro.

## Como funciona

A classe `RoundRobin`, em `politcs.py`, mantém a posição da próxima escolha.
Cada chamada a `select_server()`:

1. Obtém o lock da tabela.
2. Avança circularmente, examinando no máximo uma volta na lista.
3. Retorna `(ip, porta)` do primeiro servidor `UP` encontrado.
4. Retorna `None` se a lista estiver vazia ou todos estiverem `DOWN`.

O `with` libera o lock automaticamente, inclusive ao retornar. Assim, duas threads
não disputam a mesma posição. A ordem depende de qual thread obtém o lock primeiro,
não necessariamente de qual cliente terminou de conectar primeiro.

## Executar com vários servidores

Com servidores TCP já ouvindo nas portas 9001, 9002 e 9003:

```sh
python3 load_balance.py --port 8000 --policy round-robin --server 127.0.0.1:9001 --server 127.0.0.1:9002 --server 127.0.0.1:9003
```

Os clientes conectam somente em `127.0.0.1:8000`. O log indica a política ativa e
o destino de cada conexão estabelecida. `--policy` aceita `round-robin`, que também
é o padrão; a política Least Connections ainda será implementada na #7.

O comando antigo `--server-ip 127.0.0.1 --server-port 9001` continua funcionando.
Não misture esse par com `--server`; IPs/portas inválidos ou destinos duplicados
são rejeitados antes de iniciar o LB.

## Integração com o Gabriel

A política recebe uma lista de objetos com `ip`, `port` e `status_server`, os
mesmos campos da classe `Server` dele. Nenhuma segunda classe de servidor foi
criada. A lista é consultada diretamente a cada escolha.

O monitoramento e a política precisam usar o **mesmo lock**:

```python
table_lock = threading.Lock()
policy = RoundRobin(servers, lock=table_lock)
balancer = LoadBalancer(("127.0.0.1", 8000), policy.select_server)

# No monitoramento, ao detectar a falha:
with table_lock:
    servers[1].status_server = "DOWN"
```

Leitura de status e avanço da posição são feitos juntos, dentro do lock.
Operações de rede ficam fora dele. Um servidor recuperado volta a participar das
escolhas quando seu status é atualizado para `UP`; conexões já escolhidas mantêm
seu destino.

Até integrar a tabela da #5, a CLI monta registros provisórios com os destinos
informados e status inicial `UP`. Isso não verifica a saúde dos servidores.
A origem definitiva continua sendo a tabela cadastrada, conforme combinado;
monitoramento e recuperação de processos ficam nas #8/#9.
Contadores não fazem parte desta política e continuam na #5.

## Verificação

```sh
python3 -m unittest discover -s testes -v
```

Os testes verificam sequência, retorno ao começo, servidores `DOWN`, lista vazia,
recuperação de status e chamadas concorrentes. Também usam sockets reais através
do LB: clientes recebem identificação do servidor escolhido, destinos `DOWN` são
ignorados e doze clientes simultâneos se distribuem igualmente entre dois servidores.

Para apresentar: “Guardamos a posição do próximo servidor. A cada nova conexão,
andamos pela lista até achar um servidor UP. Ao chegar no fim, voltamos ao começo.
O lock garante que apenas uma thread por vez faça essa escolha.”
