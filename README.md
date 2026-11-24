# Load Balancer TCP

Python 3.10 ou superior, usando a biblioteca padrão.

Inicie os servidores, o monitor e o balanceador pelo menu:

```sh
python3 menu.py --policy round-robin
```

Use `--policy least-connections` para a outra política. Em outro terminal:

```sh
python3 clients.py --host 127.0.0.1 --port 8000 -M 6 -K 5
```

`M` é a quantidade de clientes e `K` a duração em segundos. Para variar a duração
entre clientes, acrescente `--duration-step 1`.

O cadastro fica em `servers/tabela_servidores.json`; use `--servers-file ARQUIVO`
para indicar outra cópia. `_enable` usa `YES/NO`, e a disponibilidade usa `UP/DOWN`.
Um servidor novo começa desabilitado: ative e habilite pelo menu.

Alterações pelo menu são aplicadas diretamente. Depois de editar o JSON por fora,
use **13 — Configurações → 8 — Recarregar tabela do JSON**.

Para verificar a execução:

```sh
python3 -m unittest discover -s testes -q
python3 demo.py --policy round-robin
python3 demo.py --policy least-connections
```
