# Integrador Sankhya-Olist

## Descrição

Este projeto é uma solução de integração entre o ERP Sankhya (SNK) e a plataforma de marketplace Olist. O objetivo principal é automatizar a troca de informações entre os dois sistemas, otimizando processos de e-commerce como sincronização de produtos, estoque, pedidos e faturamento.

## Funcionalidades

A integração contempla as seguintes funcionalidades:

*   **Sincronização de Produtos:** Envio de produtos do Sankhya para a Olist.
*   **Sincronização de Estoque:** Atualização automática do estoque dos produtos na Olist com base nos níveis do Sankhya.
*   **Importação de Pedidos:** Criação de pedidos de venda no Sankhya a partir das vendas realizadas na Olist.
*   **Atualização de Status do Pedido:** Realiza o faturamento dos pedidos no Olist após validação no Sankhya.

## Estrutura de Diretório

A estrutura de diretórios do projeto está organizada da seguinte forma:

```bash
olist-integrador/
├── alembic/
│   ├── versions/    # Migrations geradas (histórico de alterações do schema)
│   └── env.py       # Arquivo de configuração do alembic
├── database/
│   ├── crud/        # Funções para interação com o banco de dados
│   ├── __main__.py  # Inicializa o banco de dados
│   ├── database.py  # Arquivo principal do banco de dados
│   ├── models.py    # Modelos do banco de dados
│   └── schemas.py   # Schemas do banco de dados
├── keys/            # Variáveis de ambiente e credenciais
├── logs/            # Logs da aplicação
├── routers/         # Rotas da API (execução das rotinas de integração)
├── src/
│   ├── integrador/  # Rotinas de integração
│   ├── json/        # Estruturas de dados para chamadas da API Olist
│   ├── olist/       # Funções para interação com a API Olist
│   ├── parser/      # Funções para traduzir o formato dos dados entre APIs
│   ├── sankhya/     # Funções para interação com a API Sankhya
│   ├── scheduler/   # Orquestração dos jobs
│   ├── services/    # Serviços de busca de CEP, envio de E-mail, criptografia
│   ├── sql/         # Scripts SQL para consultas específicas no banco de dados
│   └── utils/       # Funções auxiliares
├── app.py           # Configura o integrador como API
├── __main__.py      # Inicializa o servidor
├── alembic.ini       # Configuração do Alembic (migrations)
├── docker-compose.yml # Sobe um PostgreSQL local para homologação/testes
├── .gitignore
├── README.md
└── requirements.txt
```

## Pré-requisitos

Antes de começar, certifique-se de ter os seguintes pré-requisitos instalados e configurados:

*   Python 3.9+
*   PostgreSQL — local/próprio, **ou** Docker + Docker Compose para subir um banco de homologação (veja [Ambiente de homologação com Docker](#ambiente-de-homologação-com-docker))
*   Credenciais da API do ERP Sankhya.
*   Credenciais da API da Olist.

## Instalação

1.  Clone o repositório:
    ```bash
    git clone https://github.com/tieisen/olist-integrador.git
    cd olist-integrador
    ```

2.  Crie um ambiente:
    ```bash
    python -m venv venv
    ```

4.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

5.  Crie um arquivo `keys/.env` com base no arquivo `example.env`. Se for usar o PostgreSQL local via Docker (passo abaixo), aponte `POSTGRES_URL`/`ALEMBIC_URL` para `localhost:5432` com o usuário `olist`/`olist` definido no `docker-compose.yml`.

6.  Garanta que o banco de dados (o schema, não as tabelas) exista. O comando abaixo cria o banco definido em `DB_NAME` caso ele ainda não exista:
    ```bash
    cd olist-integrador
    python -m database
    ```
    > Esse comando também cria as tabelas via `create_all`. Se você pretende versionar o schema com Alembic (recomendado a partir de agora), pule a criação das tabelas por aqui e siga o passo 7 — ele já cria toda a estrutura a partir da migration baseline.

7.  Aplique as migrations do Alembic para criar/atualizar as tabelas:
    ```bash
    alembic upgrade head
    ```
    Veja mais detalhes em [Migrations (Alembic)](#migrations-alembic).

8.  Inicialize a aplicação:
    ```bash
    cd c:/repos/olist-integrador
    call venv\Scripts\activate
    python .
    ```
9.  Teste acessando o endereço `http://[IP]:[PORTA]/docs`. Você deve visualizar a documentação da API com a funcão de cada rota.
    ```
    Dica: Inicie cadastrando uma empresa
    ```

## Ambiente de homologação com Docker

O `docker-compose.yml` na raiz do projeto sobe um PostgreSQL local (imagem `postgres:16-alpine`) para uso em desenvolvimento/homologação, sem depender do servidor de produção.

```bash
docker compose up -d
```

Isso cria um container `olist-integrador-postgres-homolog` na porta `5432`, com usuário/senha `olist`/`olist` e um volume nomeado (`olist_homolog_data`) que persiste os dados entre reinícios. Configure `keys/.env` apontando para esse container:

```env
POSTGRES_URL = "postgresql+asyncpg://olist:olist@localhost:5432/"
ALEMBIC_URL = "postgresql+psycopg2://olist:olist@localhost:5432/"
DB_NAME = "olist_homolog"
```

Para reiniciar do zero (apaga todos os dados): `docker compose down -v`.

## Migrations (Alembic)

O schema do banco é versionado com [Alembic](https://alembic.sqlalchemy.org/), usando as mesmas credenciais (`ALEMBIC_URL` + `DB_NAME`) do `keys/.env`. O histórico de migrations fica em `alembic/versions/` e **deve ser commitado** — é o que garante que todo ambiente (sua máquina, homologação, produção) aplique exatamente o mesmo schema.

*   **Banco novo, vazio:** `alembic upgrade head` sozinho já cria toda a estrutura (a primeira migration é uma *baseline* com o `CREATE TABLE` de tudo).
*   **Banco que já existe e já está com o schema em dia** (ex.: foi criado via `python -m database` antes de adotar Alembic): não rode `upgrade`, rode `alembic stamp head` — isso só marca o banco como estando na revision mais recente, sem executar nenhum DDL.
*   **Depois de alterar `database/models.py`:** gere uma nova migration e revise o arquivo gerado antes de aplicar:

    ```bash
    alembic revision --autogenerate -m "descrição da mudança"
    alembic upgrade head
    ```

> ⚠️ Nunca edite ou apague um arquivo de migration depois que ele for commitado e compartilhado (main, outro ambiente). Se precisar corrigir algo, crie uma nova migration em cima da existente.

## Interface
A interface (front-end) do projeto está disponível [neste repositório](https://github.com/tieisen/olist-painel)
