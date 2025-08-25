# Integrador Sankhya-Olist

## 📖 Descrição

Este projeto é uma solução de integração entre o ERP Sankhya (SNK) e a plataforma de marketplace Olist. O objetivo principal é automatizar a troca de informações entre os dois sistemas, otimizando processos de e-commerce como sincronização de produtos, estoque, pedidos e faturamento.

## ✨ Funcionalidades

A integração contempla as seguintes funcionalidades:

*   **📦 Sincronização de Produtos:** Envio de produtos do Sankhya para a Olist, incluindo descrição, preço, dimensões e imagens.
*   **📊 Sincronização de Estoque:** Atualização automática do estoque dos produtos na Olist com base nos níveis do Sankhya.
*   **🛒 Importação de Pedidos:** Criação de pedidos de venda no Sankhya a partir das vendas realizadas na Olist.
*   **🚚 Atualização de Status do Pedido:** Realiza o faturamento dos pedidos no Olist após validação no Sankhya.

## 🗺️ Estrutura de Diretório

A estrutura de diretórios do projeto está organizada da seguinte forma:

```bash
olist-integrador/
├── database/
│   ├── backups/     # Backups do banco de dados
│   ├── crud/        # Funções para interação com o banco de dados
│   ├── models.py    # Modelos do banco de dados
│   ├── schemas.py   # Esquemas Pydantic para validação de dados
│   └── database.py  # Arquivo principal do banco de dados
├── keys/            # Variáveis de ambiente e credenciais
├── logs/            # Logs da aplicação
├── routers/         # Rotas da API (execução das rotinas de integração)
├── routines/        # Rotinas de integração
├── run/             # Arquivos .bat para agendamento
├── src/
│   ├── integrador/  # Rotinas de integração
│   ├── json/        # Estruturas de dados para chamadas da API Olist
│   ├── olist/       # Funções para interação com a API Olist
│   ├── parser/      # Funções para traduzir o formato dos dados entre APIs
│   ├── sankhya/     # Funções para interação com a API Sankhya
│   ├── services/    # Serviços de busca de CEP e envio de E-mail
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

A configuração da integração é feita, através de variáveis de ambiente `.env`. \
Utilize o arquivo `example.env` como base.

## ▶️ Uso

Para rodar a integração, execute o arquivo `__main__.py`:

```bash
cd olist-integrador
call venv\Scripts\activate
python .
```
Teste acessando o endereço `http://[IP]:[PORTA]/docs`. Você deve visualizar a documentação da API com a funcão de cada rota.

Recomenda-se agendar a execução dos scripts na pasta `run` utilizando ferramentas como o `cron` (Linux/macOS) ou o Agendador de Tarefas (Windows) para manter os sistemas sincronizados em intervalos regulares.

## 🤝 Contribuição

Contribuições são muito bem-vindas! Se você deseja melhorar este projeto, por favor, siga os passos:

1.  Faça um fork do projeto.
2.  Crie uma nova branch (`git checkout -b feature/sua-melhoria`).
3.  Faça commit das suas alterações (`git commit -m 'Adiciona sua-melhoria'`).
4.  Faça push para a branch (`git push origin feature/sua-melhoria`).
5.  Abra um Pull Request.