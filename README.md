# PostgreSQL Lock Monitor

Dashboard demonstrativo em Flask para identificar sessões PostgreSQL bloqueadas e suas respectivas sessões bloqueadoras em múltiplas instâncias.

> **Aviso:** este repositório é uma implementação demonstrativa/reconstruída para portfólio. Não contém credenciais, hosts reais, dados empresariais ou informações confidenciais de empregadores anteriores. Todas as instâncias fornecidas como exemplo são fictícias.

## Como funciona

O backend consulta `pg_stat_activity` em conjunto com `pg_blocking_pids()` para identificar relações de bloqueio. As instâncias são verificadas em paralelo, o resultado é mantido em cache e a interface apresenta o estado consolidado da última coleta.

## Informações exibidas

- PID da sessão bloqueada
- Usuário bloqueado
- PID da sessão bloqueadora
- Usuário bloqueador
- Tempo de espera
- Consulta bloqueada
- Consulta bloqueadora
- Status de conexão/coleta por instância

## Tecnologias

Python, Flask, psycopg2, PostgreSQL, ThreadPoolExecutor e HTML/CSS/JavaScript.

## Configuração

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

Copie `.env.example` para `.env`, ajuste as variáveis e copie `config/pswd.example` para `config/pswd` somente no seu ambiente local. Preencha `config/instances.txt` com hosts PostgreSQL autorizados.

```bash
python app.py
```

## Consulta principal

O monitor utiliza recursos nativos do PostgreSQL, principalmente `pg_stat_activity` e `pg_blocking_pids()`, evitando qualquer alteração nos dados monitorados.

## Segurança

`config/pswd`, `.env`, logs, caches Python e ambientes virtuais estão ignorados pelo Git. Use uma conta PostgreSQL com o menor privilégio necessário e nunca publique credenciais reais.

## Autor

Gabriel Munhoz — projeto demonstrativo de portfólio.
