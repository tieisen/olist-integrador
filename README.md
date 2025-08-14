# Integrador SNK-Olist

## 📖 Descrição

Este projeto é uma solução de integração entre o ERP Sankhya (SNK) e a plataforma de marketplace Olist. O objetivo principal é automatizar a troca de informações entre os dois sistemas, otimizando processos de e-commerce como sincronização de produtos, estoque, pedidos e faturamento.

## ✨ Funcionalidades

A integração contempla as seguintes funcionalidades (adapte conforme necessário):

*   **📦 Sincronização de Produtos:** Envio de produtos do Sankhya para a Olist, incluindo descrição, preço, dimensões e imagens.
*   **📊 Sincronização de Estoque:** Atualização automática do estoque dos produtos na Olist com base nos níveis do Sankhya.
*   **🛒 Importação de Pedidos:** Criação de pedidos de venda no Sankhya a partir das vendas realizadas na Olist.
*   **🚚 Atualização de Status do Pedido:** Envio de informações de faturamento (nota fiscal) e dados de rastreamento do Sankhya para a Olist.

## 🗺️ Estrutura de Diretório

A estrutura de diretórios do projeto está organizada da seguinte forma:

```bash
olist-integrador/
├── database/             # Contém a estrutura de banco de dados
│   ├── backups/          # Backups do banco de dados
│   ├── crud/             # Funções para interação com o banco de dados
│   ├── models/           # Modelos Pydantic para validação de dados
│   ├── schemas/          # Esquemas Pydantic para validação de dados
│   ├── database.py       # Arquivo principal do banco de dados
│   └── dependencies.py   # Dependências do banco de dados
├── keys/                 # Variáveis de ambiente e credenciais
├── logs/                 # Logs da aplicação
├── routers/              # Rotas da API (execução das rotinas de integração)
├── routines/             # Rotinas de integração
├── run/                  # Arquivos .bat para agendamento
├── src/                  # Código principal da aplicação
│   ├── integrador/       # Rotinas de integração
│   ├── json/             # Estruturas de dados para chamadas da API Olist
│   ├── olist/            # Funções para interação com a API Olist
│   ├── parser/           # Funções para traduzir o formato dos dados entre APIs
│   ├── sankhya/          # Funções para interação com a API Sankhya
│   ├── services/         # Serviços de busca de CEP e envio de E-mail
│   └── utils/            # Funções auxiliares
├── __init__.py           # Inicia a API
├── .gitignore            # Arquivos e pastas a serem ignorados pelo Git
├── README.md             # Documentação do projeto
└── requirements.txt      # Lista de dependências Python
```

## 🚀 Pré-requisitos

Antes de começar, certifique-se de ter os seguintes pré-requisitos instalados e configurados:

*   Python 3.9+
*   Pip
*   Acesso e credenciais para a API do ERP Sankhya.
*   Credenciais da API da Olist (`App-Id` e `App-Secret`).

## ⚙️ Instalação

1.  Clone o repositório:
    ```bash
    git clone https://github.com/tieisen/olist-integrador.git
    cd olist-integrador
    ```

2.  Instale as dependências (exemplo para Python):
    ```bash
    pip install -r requirements.txt
    ```

## 🔧 Configuração

A configuração da integração é feita, preferencialmente, através de variáveis de ambiente ou de um arquivo de configuração (ex: `.env` ou `config.ini`).

**Exemplo de variáveis necessárias:**

```
# Sankhya
SANKHYA_API_URL="http://seu.sankhya.com/mge"
SANKHYA_USER="seu_usuario"
SANKHYA_PASSWORD="sua_senha"

# Olist
OLIST_APP_ID="seu_app_id"
OLIST_APP_SECRET="seu_app_secret"
```

> **⚠️ Segurança:** Nunca adicione arquivos com senhas e chaves secretas (como `.env` ou `config.ini`) ao controle de versão do Git. Adicione-os ao seu arquivo `.gitignore`.

## ▶️ Uso

Para executar a integração, utilize o script principal:

```bash
# Exemplo de como executar o script
uvicorn __init__:app --host=[IP] --port=[PORTA] --reload
```
Acesse `http://localhost:[PORTA]/docs` para buscar a documentação da API com a funcão de cada rota.

Recomenda-se agendar a execução deste script utilizando ferramentas como o `cron` (Linux/macOS) ou o Agendador de Tarefas (Windows) para manter os sistemas sincronizados em intervalos regulares.

## 🤝 Contribuição

Contribuições são muito bem-vindas! Se você deseja melhorar este projeto, por favor, siga os passos:

1.  Faça um fork do projeto.
2.  Crie uma nova branch (`git checkout -b feature/sua-melhoria`).
3.  Faça commit das suas alterações (`git commit -m 'Adiciona sua-melhoria'`).
4.  Faça push para a branch (`git push origin feature/sua-melhoria`).
5.  Abra um Pull Request.