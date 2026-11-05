# Encaminhamento TCP concorrente — issue #4

O LB recebe conexões de clientes, escolhe um destino uma vez por conexão e
encaminha os bytes nos dois sentidos. Uma thread atende cada conexão e outra
encaminha as respostas, permitindo tráfego simultâneo nos dois sentidos e vários
clientes conectados. O limite padrão é de 128 clientes; conexões excedentes são
encerradas sem interromper as existentes.

## Como explicar o funcionamento

Pense no LB como uma ponte: o cliente conversa com o LB, e o LB conversa com o
servidor escolhido. O cliente nunca precisa conhecer o endereço desse servidor.

```text
cliente -> LB -> servidor escolhido
cliente <- LB <- servidor escolhido
```

O fluxo principal está nestes métodos de `load_balance.py`:

1. `serve_forever()` espera novas conexões com `accept()`.
2. `_start_client()` registra cada cliente e inicia sua thread de atendimento.
3. `_handle_client()` chama `select_server()` uma vez e conecta ao destino.
4. `_relay()` lê com `recv()` e repassa com `sendall()`, repetindo até o fim.
5. Ao terminar, o `finally` fecha os sockets e chama `release_server()`, se houver.

Cada cliente usa duas threads: uma para levar seus dados ao servidor e outra para
trazer as respostas. Isso permite receber respostas enquanto o cliente ainda envia
dados. A thread principal continua aceitando novos clientes.

O `Lock` permite que apenas uma thread por vez altere o registro de conexões e
threads ativas. Também protege a verificação do limite de clientes. Ele não fica
preso durante `recv()` ou `sendall()`, então clientes diferentes continuam sendo
atendidos simultaneamente.

`shutdown()` sinaliza o encerramento, fecha a escuta e os sockets ativos e espera
as threads terminarem. Fechar os sockets desbloqueia as threads que estiverem
esperando dados. Isso evita que o programa fique preso ao sair.

Resumo para apresentação: “O LB aceita vários clientes ao mesmo tempo. Para cada
conexão, escolhe um servidor e mantém esse destino até o fim. Duas threads copiam
os dados nos dois sentidos. Quando a conexão acaba, os recursos são liberados.”

## Executar esta etapa

Com um servidor TCP já ouvindo em `127.0.0.1:9001`:

```sh
python3 load_balance.py --host 127.0.0.1 --port 8000 --server-ip 127.0.0.1 --server-port 9001
```

Os clientes devem conectar somente em `127.0.0.1:8000`. O destino fixo da CLI
permite executar a #4 enquanto tabela, servidores e políticas são desenvolvidos.
Ele não implementa distribuição de carga. `Ctrl+C` fecha a escuta, interrompe
conexões ativas e espera o encerramento das threads.

## Integração com tabela e políticas

`LoadBalancer(address, select_server, release_server=None)` recebe funções:

- `select_server()` retorna `(ip, porta)` de um servidor disponível, ou `None`.
  A escolha ocorre uma única vez por conexão do cliente. Quando houver contadores,
  essa função deve escolher e reservar a conexão na mesma seção protegida por lock.
- `release_server(address)` desfaz essa reserva exatamente uma vez, inclusive em
  falha ao conectar ao destino. A liberação também precisa da sincronização da tabela.

Essas funções serão adaptadores para a tabela da #5 e as políticas das #6/#7.
Elas devem ser rápidas e não bloquear em operações de rede. Nenhuma cópia da classe
`Server` foi adicionada; a branch do Gabriel pode evoluir sem dependência circular.
Os contadores e o status UP/DOWN continuam sob responsabilidade da tabela/monitoramento.
Uma falha de encaminhamento não altera o status do servidor automaticamente.

O logger usa o nome `Load_balance`, igual ao logger preparado na branch `Gabriel`.
A CLI registra no terminal enquanto a configuração de logs da #11 não é integrada.

## Protocolo e encerramento

O protocolo de mensagens da #1 ainda não foi definido. Este LB preserva o fluxo
binário TCP sem interpretar delimitadores, decodificar texto ou considerar cada
`recv()` uma mensagem completa. Cliente e servidor precisam concordar com a
delimitação; o LB não acrescenta mensagens de erro ao fluxo.

Um EOF fecha somente o envio ao outro lado, permitindo receber uma resposta
depois de `shutdown(SHUT_WR)`. Erros de rede encerram os dois lados. Não há
repetição automática da requisição, que poderia duplicar dados já enviados.
O timeout de conexão ao servidor é de três segundos; conexões estabelecidas
permanecem abertas até os participantes encerrarem ou o LB ser desligado.

## Verificação

Somente biblioteca padrão do Python 3; sem dependências adicionais:

```sh
python3 -m unittest discover -s testes -p 'test_load_balance.py' -v
```

Os testes usam sockets TCP reais em portas locais temporárias. Cobrem concorrência,
roteamento por conexão, dados binários/maiores que o buffer, escritas fragmentadas,
respostas antes do envio do cliente, EOF parcial, indisponibilidade, liberação de
reservas, limite de clientes e desligamento com conexões ociosas.

A integração com a tabela do Gabriel, a decisão de protocolo da #1 e a execução
nos servidores do DInf continuam pendentes. Esta entrega cobre o transporte da #4.
