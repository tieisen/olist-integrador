# Integrador Sankhya-Olist

## 📖 Descrição

Este projeto é uma solução de integração entre o ERP Sankhya (SNK) e a plataforma de marketplace Olist. O objetivo principal é automatizar a troca de informações entre os dois sistemas, otimizando processos de e-commerce como sincronização de produtos, estoque, pedidos e faturamento.

## ✨ Funcionalidades

A integração contempla as seguintes funcionalidades:

*   **📦 Sincronização de Produtos:** Envio de produtos do Sankhya para a Olist.
*   **📊 Sincronização de Estoque:** Atualização automática do estoque dos produtos na Olist com base nos níveis do Sankhya.
*   **🛒 Importação de Pedidos:** Criação de pedidos de venda no Sankhya a partir das vendas realizadas na Olist.
*   **🚚 Atualização de Status do Pedido:** Realiza o faturamento dos pedidos no Olist após validação no Sankhya.

## 🗺️ Estrutura de Diretório

A estrutura de diretórios do projeto está organizada da seguinte forma:

```bash
olist-integrador/
├── alembic/
│   └── env.py       # Arquivo de configuração do alembic
├── database/
│   ├── crud/        # Funções para interação com o banco de dados
│   ├── __main__.py  # Inicializa o banco de dados
│   ├── database.py  # Arquivo principal do banco de dados
│   └── models.py    # Modelos do banco de dados
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
│   └── utils/       # Funções auxiliares
├── app.py           # Configura o integrador como API
├── __main__.py      # Inicializa o servidor
├── .gitignore
├── README.md
└── requirements.txt
```

## 🚀 Pré-requisitos

Antes de começar, certifique-se de ter os seguintes pré-requisitos instalados e configurados:

*   Python 3.9+
*   PostgreSQL
*   Credenciais da API do ERP Sankhya.
*   Credenciais da API da Olist.

## ⚙️ Instalação

1.  Clone o repositório:
    ```bash
    git clone https://github.com/tieisen/olist-integrador.git
    cd olist-integrador
    ```

2.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

3.  Crie um arquivo `keys/.env` com base no arquivo `example.env`

4.  No diretório do projeto, execute a criação do banco de dados:
    ```bash
    cd olist-integrador
    python -m database
    ```

5.  Inicialize a aplicação:
    ```bash
    cd c:/repos/olist-integrador
    call venv\Scripts\activate
    python .
    ```
6.  Teste acessando o endereço `http://[IP]:[PORTA]/docs`. Você deve visualizar a documentação da API com a funcão de cada rota.
    ```
    ✨Dica: Inicie cadastrando uma empresa
    ```

## Interface
A interface (front-end) do projeto está disponível [neste repositório](https://github.com/tieisen/olist-painel)

## 🤝 Contribuição

Contribuições são muito bem-vindas! Se você deseja melhorar este projeto, por favor, siga os passos:

1.  Faça um fork do projeto.
2.  Crie uma nova branch (`git checkout -b feature/sua-melhoria`).
3.  Faça commit das suas alterações (`git commit -m 'Adiciona sua-melhoria'`).
4.  Faça push para a branch (`git push origin feature/sua-melhoria`).
5.  Abra um Pull Request.