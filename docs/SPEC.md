# Especificação Técnica — Olist Integrador

> Documento técnico da integração entre o ERP **Sankhya** (SNK) e a plataforma **Olist/Tiny** (API v3).
> Gerado a partir da leitura do código-fonte em 2026-10-06 (commit `be55479`). Referências no formato `arquivo:linha`.

## Sumário

1. [Visão geral](#1-visão-geral)
2. [Stack e execução](#2-stack-e-execução)
3. [Arquitetura](#3-arquitetura)
4. [Configuração (variáveis de ambiente)](#4-configuração-variáveis-de-ambiente)
5. [Modelo de dados](#5-modelo-de-dados)
6. [Camada de persistência (CRUD)](#6-camada-de-persistência-crud)
7. [Decoradores](#7-decoradores)
8. [Clientes de API externos](#8-clientes-de-api-externos)
9. [Fluxos de negócio](#9-fluxos-de-negócio)
10. [API HTTP](#10-api-http)
11. [Agendador (scheduler)](#11-agendador-scheduler)
12. [Logging, auditoria e notificação](#12-logging-auditoria-e-notificação)
13. [Segurança](#13-segurança)
14. [Scripts utilitários](#14-scripts-utilitários)
15. [Limitações conhecidas e débitos técnicos](#15-limitações-conhecidas-e-débitos-técnicos)

---

## 1. Visão geral

O serviço sincroniza, entre Sankhya e Olist:

| Domínio | Direção | Disparo |
|---|---|---|
| Produtos (cadastro) | Sankhya ⇄ Olist | Scheduler + `POST /produtos/integrar` |
| Estoque | Sankhya → Olist | Scheduler + `POST /estoque/integrar` |
| Pedidos (recebimento) | Olist → base local | Scheduler + endpoints `/pedidos/*` |
| Pedidos (importação/confirmação) | base local → Sankhya | Somente endpoints |
| Separação | Olist ⇄ base local | Scheduler (recebimento) + endpoints |
| Faturamento / NF-e | Sankhya ⇄ Olist | Somente endpoints |
| Transferência entre empresas | Sankhya → Sankhya | Somente endpoints |
| Financeiro (contas a receber/pagar) | base/marketplace → Olist | Somente endpoints |
| Devoluções | Olist → Sankhya | Somente endpoints |

- **Multi-tenant:** uma `empresa` (Sankhya, `snk_codemp`) possui N `ecommerce` (lojas Olist, `id_loja`). Os parâmetros de cada tenant (TOPs, naturezas, locais, credenciais) são lidos do banco, nunca do `.env`.
- **Integração por polling:** não existem webhooks. Toda comunicação com a Olist é iniciada pelo serviço.
- Os usuários operam pelo Sankhya. O front-end `tieisen/olist-painel` não é utilizado.

---

## 2. Stack e execução

| Componente | Tecnologia |
|---|---|
| Linguagem | Python 3.13 (`pyproject.toml`, alvo `py313`) |
| API | FastAPI 0.116 + Uvicorn 0.35 |
| ORM | SQLAlchemy 2.0 (async, `asyncpg`) |
| Migrations | Alembic 1.16 (sync, `psycopg2`) |
| Banco | PostgreSQL 16 |
| Agendador | APScheduler 3.11 (`AsyncIOScheduler` + `SQLAlchemyJobStore`) |
| HTTP | `requests` (síncrono) |
| Criptografia | `cryptography` (Fernet) |
| Automação web | Selenium 4 + Firefox (login OAuth Olist e bot) |
| Validação | Pydantic 2.8 |
| Lint | Ruff (line-length 100, regras E/F/I/UP/B) via pre-commit |

### Inicialização

1. `__main__.py:9-20` executa `load_env()`, lê `HOST`/`PORT` e chama `uvicorn.run("app:app", ...)`.
2. `app.py:21-39` define o `lifespan`:
   - **startup:** se `SCHEDULER_ENABLE` (padrão `"true"`) for `"true"`, chama `iniciar_agendador()`;
   - **shutdown:** se o agendador estiver em execução, chama `encerrar_agendador()`.
3. Middleware: apenas `CORSMiddleware` totalmente aberto (`app.py:44-51`).
4. Routers registrados em `app.py:53-60` (ver [§10](#10-api-http)).

### Ambiente local

```bash
docker compose up -d        # postgres:16-alpine, olist/olist, porta 5432, volume olist_homolog_data
alembic upgrade head        # aplica a baseline
python -m database.dev.populate_db   # popula a partir de CSVs em database/dev/data/ (gitignored)
python .                    # sobe a API
```

`keys/.env` é carregado por caminho relativo, então os comandos devem ser executados a partir da raiz.

---

## 3. Arquitetura

```
routers/            → endpoints FastAPI (entrada manual)
src/scheduler/      → APScheduler + jobs (entrada automática)
   └─ jobs/         → orquestração por empresa/ecommerce (compartilhado com routers)
src/integrador/     → regras de negócio por domínio
   ├─ src/olist/    → cliente HTTP Olist/Tiny API v3
   ├─ src/sankhya/  → cliente HTTP Sankhya (gateway)
   ├─ src/parser/   → conversão de payloads Olist ⇄ Sankhya
   └─ database/crud → persistência local (estado da integração)
src/services/       → Shopee, ViaCEP, SMTP, criptografia, bot Selenium
src/utils/          → decoradores, paginação, formatação, log, validação
src/sql/            → scripts SQL executados via DbExplorer (gitignored: *.sql)
src/json/           → templates de payload Olist
```

**Fluxo de chamada:** `router` → `job` → `Integrador` → (`cliente Olist` | `cliente Sankhya` | `parser` | `crud`).

Os endpoints manuais e o scheduler chamam as **mesmas** funções de `src/scheduler/jobs/`.

### Iteração multi-tenant nos jobs

```python
for empresa in crud.empresa.buscar():  # empresas ativas
    for ecommerce in crud.ecommerce.buscar(empresa_id=empresa.id):  # lojas ativas
        Integrador(id_loja=ecommerce.id_loja, codemp=empresa.snk_codemp).metodo()
```

`src/scheduler/jobs/faturamento.py` (`_por_empresa`, `_por_loja`, `_retorno`) isola os erros por e-commerce e devolve `{"status": bool, "exception": "msg1; msg2"}`.

---

## 4. Configuração (variáveis de ambiente)

Todas as variáveis vêm de `keys/.env` (modelo: `keys/example.env`), carregado por `src/utils/load_env.py`. O Alembic carrega o mesmo arquivo com `encoding='latin-1'` (`alembic/env.py:10`).

### Aplicação e infraestrutura

| Variável | Uso |
|---|---|
| `HOST`, `PORT` | Bind do Uvicorn |
| `API_TITLE`, `API_DESCRIPTION`, `API_VERSION` | Metadados do FastAPI |
| `POSTGRES_URL` | URL `postgresql+asyncpg://...` sem nome do banco |
| `ALEMBIC_URL` | URL `postgresql+psycopg2://...` sem nome do banco (Alembic e jobstore) |
| `DB_NAME` | Nome do banco |
| `SCHEDULER_ENABLE` | Liga/desliga o agendador (padrão `true`) |
| `MINUTOS_JOB_PADRAO` | Intervalo do job principal. **Obrigatório:** lido no import do scheduler, então a API não sobe sem ele |
| `REQ_TIME_SLEEP` | Pausa entre requisições (padrão 1.5 s) |
| `LOGGER_FORMAT` | Formato do `logging` |
| `TEMPO_HISTORICO_MINUTOS` | Janela de falhas para a notificação |
| `DIAS_LIMPA_CACHE` | Retenção de logs (multiplicada por 4) |
| `PATH_FERNET_KEY` | Caminho da chave Fernet (`keys/.fernet.key`) |
| `AMBIENTE` | `snd` = sandbox: desativa escritas de observação no Olist e usa marcadores de teste |

### SMTP

`SMTP_SERVER`, `SMTP_PORT`, `SENDER_MAIL`, `SENDER_PASSWORD`, `TO_DEFAULT`, `BODY_HTML`, `MAIL_SUBJECT`, `MAIL_COLOR`.

### Olist

| Variável | Uso |
|---|---|
| `OLIST_AUTH_URL`, `OLIST_ENDPOINT_TOKEN`, `OLIST_REDIRECT_URI` | OAuth2/OIDC (Keycloak Tiny) |
| `OLIST_API_URL` | `https://api.tiny.com.br/public-api/v3` |
| `OLIST_ENDPOINT_{PRODUTOS,ESTOQUES,PEDIDOS,NOTAS,FINANCEIRO_RECEBER,FINANCEIRO_PAGAR,SEPARACAO}` | Paths dos recursos |
| `OLIST_TEMPO_BUSCA_ALTER_PROD` | Janela (minutos) para buscar alterações de produto (padrão 30) |
| `OLIST_SIT_PEDIDO_CANCELADO`, `OLIST_SIT_PEDIDO_INCOMPLETO` | IDs de situação de pedido |
| `OLIST_OBS_MVTO_ESTOQUE` | Observação nas movimentações de estoque |
| `OBJECT_PRODUTO`, `OBJECT_ESTOQUE` | Caminhos dos templates `src/json/*.json` |
| `OLIST_BOT_*`, `OLIST_URL_*`, `OLIST_TIMEOUT_LANCA_LOTES` | Bot Selenium (sem uso no fluxo atual) |

### Sankhya

| Variável | Uso |
|---|---|
| `SANKHYA_APP_ID` | Aplicação usada pelo serviço de autenticação (global) |
| `SANKHYA_URL_AUTH` | Serviço intermediário de token (rede interna) |
| `SANKHYA_URL_{LOAD_RECORDS,SAVE,DELETE,DBEXPLORER,PEDIDO,PEDIDO_ALTERA_ITEM,CONFIRMA_PEDIDO,FATURA_PEDIDO}` | Endpoints do gateway |
| `SANKHYA_TABELA_{PRODUTO,RASTRO_PRODUTO,RASTRO_ESTOQUE,RELATORIO}` | Tabelas customizadas (`AD_*`) |
| `SANKHYA_VIEW_SALDO_ESTOQUE` | View `AD_OLISTESTOQUE` |
| `SANKHYA_CRITERIOS_NOTA_TRANSFERENCIA` | Critério de busca de nota de transferência |
| `SANKHYA_PATH_SCRIPT_*`, `SANKHYA_PATH_RELATORIO_SEPARACAO` | Caminhos dos arquivos `.sql` |

### Marketplaces

| Variável | Uso |
|---|---|
| `SHOPEE_HOST_URL`, `SHOPEE_PATH_{AUTH,TOKEN,REFRESH_TOKEN,INCOME_DETAIL,ESCROW_DETAIL}` | API Shopee |
| `BLZWEB_TAXA_ENVIO`, `BLZWEB_TAXA_COMISSAO` | Cálculo de repasse da Beleza na Web |

---

## 5. Modelo de dados

Definido em `database/models.py`. Migration única: `alembic/versions/a258aa95113f_baseline_estrutura_inicial_do_banco.py`.

> **Produção:** o schema foi alterado manualmente e não está versionado. Antes de qualquer `alembic upgrade`, compare o schema com `models.py` e execute `alembic stamp head`.

### Diagrama de relacionamentos

```
empresa 1─N ecommerce 1─N pedido 1─N nota 1─N devolucao
   │            └─1─N shopee            │
   ├─1─N produto ─────────┐             └─1─N log_pedido
   ├─1─N olist (tokens)   │
   └─1─N log ─┬─N log_produto (N─1 produto)
              ├─N log_estoque
              └─N log_pedido (N─1 pedido)

sankhya (isolada; token global)
```

Todas as FKs usam `ON DELETE CASCADE`. Timestamps são `DateTime(timezone=True)`.

### Tabelas

#### `empresa` — tenant Sankhya
| Grupo | Colunas |
|---|---|
| Identificação | `id` PK, `snk_codemp` (NN), `nome` (NN), `cnpj` (NN), `snk_codemp_fornecedor` (NN), `serie_nfe`, `ativo` (default True), `dh_criacao`, `dh_atualizacao` |
| Credenciais Olist | `client_id`, `client_secret`🔒, `olist_admin_email`, `olist_admin_senha`🔒 |
| Parâmetros Olist | `olist_id_fornecedor_padrao`, `olist_id_deposito_padrao`, `olist_dias_busca_pedidos`, `olist_situacao_busca_pedidos`, `olist_id_conta_destino`, `olist_id_categoria_padrao`, `olist_id_categoria_despesa_padrao`, `olist_id_categoria_taxa_padrao`, `olist_id_categoria_frete_padrao`, `olist_id_marca_padrao` |
| Credenciais Sankhya | `snk_token`🔒, `snk_appkey`🔒, `snk_admin_email`, `snk_admin_senha`🔒, `snk_timeout_token_min` |
| TOPs Sankhya | `snk_top_pedido`, `snk_top_venda`, `snk_top_transferencia`, `snk_top_devolucao`, `snk_top_baixa_estoque` |
| Parâmetros Sankhya | `snk_codvend`, `snk_codcencus`, `snk_codnat`, `snk_codnat_transferencia`, `snk_codtipvenda`, `snk_codusu_integracao`, `snk_codtab_transf`, `snk_codlocal_venda`, `snk_codparc`, `snk_codlocal_estoque` (str, lista), `snk_codlocal_ecommerce`, `snk_obs_transferencia` |

🔒 = criptografado com Fernet (ver [§13](#13-segurança)).

#### `ecommerce` — loja Olist
`id`, `id_loja` (NN), `nome` (NN), `empresa_id` FK, `id_fornecedor_olist`, `id_conta_destino` (NN, default 0), `id_categoria_financeiro` (NN, default 0), `id_forma_pgto_padrao`, `id_forma_rec_padrao`, `id_deposito`, `importa_pedido_lote` (default True), `limite_pedido_lote`, `ativo` (default True), `dh_criacao`, `dh_atualizacao`.

#### `produto` — vínculo de produto
`id`, `codprod` (Sankhya, NN), `idprod` (Olist), `pendencia` (bool, default False; True = alteração do Olist pendente de envio ao Sankhya), `empresa_id` FK, `dh_criacao`, `dh_atualizacao`.

#### `pedido` — estado do pedido
`id`, `id_pedido` (Olist, **UNIQUE**), `cod_pedido` (código no marketplace), `num_pedido`, `dados_pedido` (JSON completo do Olist), `id_separacao`, `nunota` (pedido Sankhya; `-1` = pedido sem necessidade de transferência), `dh_pedido`, `dh_importacao`, `dh_confirmacao`, `dh_faturamento`, `dh_cancelamento`, `erro`, `erro_descricao`, `ecommerce_id` FK.

#### `nota` — NF-e de venda
`id`, `id_nota` (Olist), `numero`, `serie`, `dh_emissao`, `chave_acesso`, `id_cliente`, `parcelado`, `nunota` (nota no Sankhya), `dh_confirmacao`, `dh_cancelamento`, `cancelado_sankhya`, `baixa_estoque_ecommerce`, `id_financeiro`, `id_financeiro_taxa`, `id_financeiro_frete`, `dh_baixa_financeiro`, `income_data` (JSONB mutável, dados de repasse do marketplace), `pedido_id` FK.

#### `devolucao` — NF de devolução
`id`, `id_nota`, `numero`, `serie`, `dh_emissao`, `chave_acesso`, `nunota`, `dh_confirmacao`, `dh_cancelamento`, `nota_id` FK.

#### `olist` — tokens OAuth por empresa
`token`🔒, `refresh_token`🔒, `id_token`🔒, `dh_expiracao_token`, `dh_expiracao_refresh_token`, `dh_solicitacao`, `empresa_id` FK. É usado o registro mais recente (`order by id desc`).

#### `sankhya` — token do gateway (global)
`app_id` PK (sem autoincrement), `x_token`🔒, `token`🔒, `dh_solicitacao`, `dh_expiracao_token`.

#### `shopee` — credenciais Shopee por e-commerce
`partner_id`, `partner_key`🔒, `shop_id`, `access_token`🔒, `refresh_token`🔒, `dh_expiracao_token`, `dh_expiracao_refresh_token`, `dh_solicitacao`, `ecommerce_id` FK.

#### `log` e filhos — auditoria
| Tabela | Colunas |
|---|---|
| `log` | `dh_execucao`, `contexto`, `de`, `para`, `sucesso`, `empresa_id` FK |
| `log_produto` | `codprod`, `idprod`, `campo`, `valor_old`, `valor_new`, `sucesso`, `obs`, `log_id`, `produto_id` |
| `log_estoque` | `codprod`, `idprod`, `qtdmov`, `sucesso`, `status_lotes`, `obs`, `log_id` |
| `log_pedido` | `evento` ∈ {`R`,`I`,`C`,`F`,`N`,`D`} (CheckConstraint), `sucesso`, `obs`, `log_id`, `pedido_id` |

Eventos de `log_pedido`: **R**ecebimento, **I**mportação, **C**onfirmação, **F**aturamento/separação, ca**N**celamento/anulação, **D**evolução.

### Máquina de estados do pedido (tabela `pedido`)

```
[Recebido] ──(id_separacao)──► [A importar] ──(nunota, dh_importacao)──► [A confirmar]
                                                                              │
                                                                       (dh_confirmacao)
                                                                              ▼
[Faturado] ◄──(dh_faturamento)── [A faturar: nunota != null, dh_faturamento null]

Qualquer estado ──(dh_cancelamento)──► [Cancelado]
crud.pedido.cancelar(nunota) → zera nunota, dh_importacao, dh_confirmacao, dh_faturamento
```

| Fila (`database/crud/pedido.py`) | Critério |
|---|---|
| `buscar_importar` | `id_separacao` preenchido, `dh_importacao` nulo, `dh_cancelamento` nulo |
| `buscar_confirmar` | `dh_importacao` preenchido, `dh_confirmacao` nulo |
| `buscar_faturar` | `nunota` preenchido, `dh_faturamento` nulo, `dh_cancelamento` nulo |
| `buscar_baixar_estoque` | nota não cancelada e `baixa_estoque_ecommerce = False` |

### Ciclo de vida da nota

`criada (id_nota)` → `emitida (chave_acesso, dh_emissao)` → `vinculada ao Sankhya (nunota)` → `confirmada (dh_confirmacao)` → `estoque e-commerce baixado (baixa_estoque_ecommerce)` → `repasse calculado (income_data)` → `lançada (id_financeiro / _taxa / _frete)` → `dh_baixa_financeiro`. Em paralelo, pode receber `dh_cancelamento`.

### Schemas Pydantic (`database/schemas.py`)

`EmpresaBase/Create/DB` e `EcommerceBase/Create/DB` usam a sintaxe Pydantic v1 (`Config.orm_mode`). Estão defasados em relação ao model: faltam `olist_id_categoria_{despesa,taxa,frete}_padrao`, `snk_top_baixa_estoque` e `snk_codlocal_ecommerce`.

---

## 6. Camada de persistência (CRUD)

Padrão comum de `database/crud/*`:

- **Escrita:** `validar_dados(modelo, kwargs, COLUNAS_CRIPTOGRAFADAS)` rejeita colunas inexistentes e criptografa os campos sensíveis.
- **Leitura:** `formatar_retorno()` converte para `dict`, descriptografa, converte o fuso para UTC-3 sem `tzinfo` e ordena as chaves.
- **Erros:** retornam `False`, `[]`, `{}` ou `None` em vez de lançar exceção.

| Módulo | Funções principais |
|---|---|
| `empresa` | `criar`, `buscar(id\|codemp)` (sem filtro → ativas), `atualizar`, `excluir` |
| `ecommerce` | `criar`, `buscar(empresa_id\|codemp\|id_loja\|ecommerce_id)`, `atualizar`, `excluir`, `buscar_dados_cadastro` (SQL cru) |
| `produto` | `criar`, `buscar`, `buscar_pendencias`, `atualizar(pendencia=...)`, `excluir` |
| `pedido` | `criar`, `buscar`, `atualizar` (suporta `lista_ids` e `nunota=-1`), filas (`buscar_importar/confirmar/checkout/faturar/baixar_estoque/reimprimir_relatorio`), `buscar_cancelar`, `cancelar`, `resetar`, `informar_erro`, `buscar_cliente` |
| `nota` | `criar`, `buscar`, `atualizar`, filas (`buscar_criar/emitir/financeiro/financeiro_parcelado/financeiro_baixar/confirmar/cancelar/...`), Shopee (`atualizarDadosContaShopee`, `atualizarDadosContaEstornoShopee`, `buscaPendenteIncomeData`, `buscarPendenteLcto`, `buscarEstornoPendenteLcto`), `validar_pedido_atendido` |
| `devolucao` | `criar` (idempotente, vincula pela chave referenciada), `buscar`, `buscar_lancar`, `buscar_confirmar`, `atualizar` |
| `olist` / `sankhya` / `shopee` | Persistência de tokens |
| `log` | `criar`, `atualizar(sucesso=None → inferido pelos filhos)`, `buscar_falhas`, `listar_falhas`, `excluir_cache` |
| `log_pedido` / `log_produto` / `log_estoque` | `criar`, `buscar_falhas`, `buscar_id` |

---

## 7. Decoradores

`src/utils/decorador.py`. As classes que usam estes decoradores precisam expor `self.contexto`, `self.id_loja`, `self.codemp` e `self.empresa_id`, além dos atributos de cache (`self.dados_*`).

| Decorador | Comportamento | Exige |
|---|---|---|
| `contexto` | Injeta `kwargs["_contexto"] = f"{self.contexto}:{func.__name__}"`, gravado em `log.contexto` | `self.contexto` |
| `carrega_dados_empresa` | Se `self.dados_empresa` estiver vazio, carrega `crud.empresa.buscar(id=empresa_id, codemp=codemp)[0]` | `self.dados_empresa`, `self.empresa_id`, `self.codemp` |
| `carrega_dados_ecommerce` | Carrega `crud.ecommerce.buscar(id_loja=self.id_loja)[0]` | `self.dados_ecommerce`, `self.id_loja` |
| `carrega_dados_shopee` | Carrega as credenciais Shopee | `self.dados_shopee`, `self.ecommerce_id`, `self.empresa_id` |
| `carrega_dados_snk` | Carrega `crud.sankhya.buscar(app_id=self.app_id)` | `self.dados_snk`, `self.app_id` |
| `interno` | Inspeciona a pilha e lança `PermissionError` se o chamador não for método da própria classe | — |
| `log_execucao` | Imprime cabeçalho e tempo de execução (`perf_counter`) no stdout | — |
| `desabilitado` | Substitui a função por um `print` | — |
| `tokenOlist` (`src/olist/autenticacao.py:303`) | Obtém/renova o token Olist e injeta `self.token` a cada chamada | `self.codemp`, `self.empresa_id` |
| `tokenSnk` (`src/sankhya/autenticacao.py:169`) | Obtém o token Sankhya e injeta `self.token` | — |

---

## 8. Clientes de API externos

Características comuns:
- Métodos `async`, mas implementados com `requests` **síncrono** e `time.sleep(REQ_TIME_SLEEP)`. Isso bloqueia o event loop.
- **Sem timeout, sem retry e sem tratamento de HTTP 429.**
- Em caso de falha, registram `logger.error` e retornam um valor falsy (`False`, `{}`, `[]`, `0`).

### 8.1 Autenticação Olist (`src/olist/autenticacao.py`)

OAuth2 Authorization Code via Keycloak do Tiny (`OLIST_AUTH_URL`).

1. **Primeiro login** (`solicitar_auth_code`, L28-64): o Selenium/Firefox abre `/auth?...`, preenche `olist_admin_email`/`olist_admin_senha` da empresa e captura o `code` no redirect.
2. **Token** (`solicitar_token`, L66-99): `POST /token`, `grant_type=authorization_code`, usando `client_id`/`client_secret` da empresa.
3. **Refresh** (`solicitar_atualizacao_token`, L101-133): `grant_type=refresh_token`.
4. **Persistência:** tabela `olist`, com `dh_expiracao_* = now + expires_in`.
5. **Decisão** (`buscar_token_salvo`, L217-242):
   - access token válido → usa;
   - só o refresh válido → renova;
   - nenhum válido → novo login com Selenium.
6. Concorrência controlada por um `asyncio.Lock` com double-check, válido **apenas dentro do processo**.

### 8.2 Autenticação Sankhya (`src/sankhya/autenticacao.py`)

- **Serviço intermediário** (não MobileLogin): `POST {SANKHYA_URL_AUTH}/{app_id}` com o header `xToken: <x_token>`. A resposta traz `token` e `dhExpiracaoToken`.
- A expiração é gravada com 1 minuto de margem. Não existe refresh: quando o token expira, um novo é solicitado.
- O token é **global**: um único `SANKHYA_APP_ID` serve a todas as empresas.
- Chamadas ao gateway usam somente `Authorization: Bearer <token>`.
- Resposta do gateway: `status` `"1"` = ok, `"0"` = erro, `"2"` = sucesso com aviso.
- A maioria das chamadas usa **`requests.get` com corpo JSON**.

### 8.3 Endpoints Olist (API v3)

| Classe | Recurso | Operações |
|---|---|---|
| `olist.Pedido` | `/pedidos` | `buscar` (por id, `numeroPedidoEcommerce`, `numero` ou cancelados), `buscar_novos` (por situação ou `dataInicial`), `atualizar_nunota` / `remover_nunota` (PUT nas observações), `adicionar_texto_erro` / `remover_texto_erro`, marcadores (`/pedidos/{id}/marcadores`: integrado, erro, parfum, teste), `gerar_nf` (`POST /pedidos/{id}/gerar-nota-fiscal`, modelo 55), `validar_kit` |
| `olist.Nota` | `/notas` | `buscar` (+ `GET /notas/{id}/xml`), `buscarData` (situações 6/7), `buscar_canceladas` (situação 3), `buscar_devolucoes` (`tipo=E`), `emitir` (`POST /notas/{id}/emitir`) |
| `olist.Produto` | `/produtos` | `buscar` (id/sku), `incluir`, `atualizar` (simples ou `/variacoes/{id}`), `buscar_todos`, `buscar_alteracoes` (`dataAlteracao`) |
| `olist.Estoque` | `/estoque` | `buscar`, `enviar_saldo` (`POST /estoque/{id}`) |
| `olist.Receita` | `/contas-receber` | `buscar`, `listarReceberAberto`, `lancar`, `baixar` (`POST /{id}/baixar`; 409 = já baixado), marcar/desmarcar devolvido |
| `olist.Despesa` | `/contas-pagar` | `buscar`, `lancar`, `baixar` |
| `olist.Separacao` | `/separacao` | `listar` (situações 1 e 4), `buscar`, `separar` (→2), `concluir` (→3, embalada) |

**Paginação** (`src/utils/busca_paginada.py:paginar_olist`): baseada em `paginacao.{limit, offset, total}`, concatenando `&offset=N` à URL.

### 8.4 Serviços Sankhya (gateway `/gateway/v1/{mge|mgecom}/service.sbr`)

| Serviço | Uso |
|---|---|
| `CRUDServiceProvider.loadRecords` | Leitura de entidades (`CabecalhoNota`, `ItemNota`, `Cidade`, `CabecalhoConferencia`, `Financeiro`, tabelas `AD_*`). Paginação por `offsetPage`/`hasMoreResult` (`paginar_snk`) |
| `DatasetSP.save` | Gravação de campos (`ItemNota.CODLOCALORIG`, `CabecalhoNota.AD_MKP_IDNFE/CHAVENFE/NUMNOTA/OBSERVACAO/NUCONFATUAL`, conferência, `AD_OLISTPRODUTO`, `AD_OLISTRELPEDIDOS`) |
| `DatasetSP.removeRecord` | Exclusão (`CabecalhoNota`, filas `AD_OLISTRAST*`, `AD_OLISTRELPEDIDOS`) |
| `DbExplorerSP.executeQuery` | Execução dos SQLs de `src/sql/` |
| `CACSP.incluirNota` | Criação de pedido, transferência, baixa de estoque e devolução sem lote |
| `CACSP.incluirAlterarItemNota` | Inclusão de item em transferência existente |
| `ServicosNfeSP.confirmarNota` | Confirmação de pedido/nota (“já foi confirmada” é tratado como sucesso) |
| `SelecaoDocumentoSP.faturar` | Faturamento do pedido (`snk_top_transferencia`) e devolução (`snk_top_devolucao`, `faturarTodosItens=false`) |

#### Classes

| Classe | Responsabilidade |
|---|---|
| `sankhya.Pedido` / `Itens` | Busca por `NUNOTA`/`AD_MKP_ID`/`AD_MKP_CODPED` (`TIPMOV='P'`), `lancar`, `confirmar`, `faturar`, `atualizar_local`, `excluir`, `buscar_cidade` (IBGE → `CODCID`), `buscar_nunota_nota` (TGFVAR) |
| `sankhya.Nota` / `Itens` | Busca, `confirmar`, `informar_numero_e_chavenfe`, `devolver`, `devolver_sem_lote`, `alterar_observacao`, `excluir` |
| `sankhya.Estoque` | Saldos (atual, por lote, por local, e-commerce por lote), fila `AD_OLISTRASTESTOQUE`, relatório `AD_OLISTRELPEDIDOS` |
| `sankhya.Produto` | `produto.sql`, gravação em `AD_OLISTPRODUTO` (`ID`, `IDPRODPAI`, `ATIVO`), fila `AD_OLISTRASTPRODUTO` |
| `sankhya.Transferencia` / `Itens` | Nota de transferência (`TIPMOV='T'`), valor por tabela de preço |
| `sankhya.Conferencia` | `TGFCON2`/`TGFCOI2`: criar, vincular ao pedido, inserir itens, concluir |
| `sankhya.Faturamento` | Itens conferidos, `compara_saldos` (local, sem HTTP) |
| `sankhya.Empresa` | Somente leitura local (`crud.empresa`) |
| `sankhya.Financeiro` | Leitura de `TGFFIN`. **Não utilizado e quebrado** (usa `self.campos` sem definição) |

#### Customizações Sankhya

| Objeto | Campos / finalidade |
|---|---|
| `TGFCAB` | `AD_MKP_ID` (id pedido Olist), `AD_MKP_CODPED` (código marketplace), `AD_MKP_NUMPED`, `AD_MKP_IDNFE`, `AD_MKP_ORIGEM` (id_loja), `AD_MKP_DESTINO`, `AD_MKP_DHCHECKOUT`, `AD_IDSHOPEE`, `AD_TAXASHOPEE` |
| `AD_OLISTPRODUTO` | Vínculo `CODPROD`+`CODPARC` → `ID`/`IDPRODPAI`/`ATIVO`, além da política de estoque (`POLITICAESTOQUE`, `TIPOBARREIRA`, `VALORBARREIRA`, `REGRABARREIRA`) |
| `AD_OLISTRASTPRODUTO` | Fila de alterações de produto (PK `CODPROD`+`CODEMP`, campo `evento` I/A) |
| `AD_OLISTRASTESTOQUE` | Fila de alterações de estoque (PK `CODPROD`+`CODEMP`) |
| `AD_OLISTRELPEDIDOS` | Relatório de separação (`ID`, `ECOMMERCE`, `CODPROD`, `DESCRICAO`, `QTD`, `UND`, `QTDSALDO`, `EMPRESA`) |
| `AD_OLISTESTOQUE` | View de saldo |

#### Scripts SQL (`src/sql/`, ignorados pelo git via `*.sql`)

Carregados por `buscar_script()` (`src/utils/buscar_arquivo.py`). Os parâmetros são interpolados com `str.format_map`, **sem bind**.

| Script | Finalidade |
|---|---|
| `buscar_estoque_atual.sql` | Saldo disponível por produto com políticas de estoque (T/V/B), barreira (P/Q), estoque mínimo e pedidos pendentes |
| `buscar_saldo_por_local.sql` | PIVOT de saldos nos locais 101 (matriz), 911 (validade curta), 102 (promoção) e 500 (e-commerce) |
| `buscar_saldo_por_lote_{item,lista}.sql` | Saldo por lote, com `AGRUPMIN` e `TIPCONTEST` |
| `buscar_saldo_ecommerce_por_lote.sql` | Saldo por lote no local do e-commerce |
| `buscar_valor_transferencia_{item,lista}.sql` | Preço de transferência (`TGFNTA`/`TGFTAB`/`TGFEXC`) |
| `buscar_itens_conferidos_{dia,pedido}.sql` | Itens conferidos (`TGFCON2`/`TGFCOI2`) |
| `buscar_aguardando_conferencia.sql` | Pedidos liberados sem conferência |
| `buscar_nunota_nota.sql` | Nota gerada a partir do pedido (`TGFVAR`) |
| `buscar_rel_separacao.sql` | Relatório atual de separação |
| `produto.sql` | `TGFPRO` LEFT JOIN `AD_OLISTPRODUTO` |

### 8.5 Serviços auxiliares (`src/services/`)

| Serviço | Descrição |
|---|---|
| `shopee.py` | Autenticação HMAC-SHA256 (`partner_id+path+timestamp[+access_token+shop_id]`), tokens por loja, `getIncomeDetail` (paginação por cursor) e `getEscrowDetail` |
| `viacep.py` | CEP → código IBGE (usado na importação de pedido único) |
| `smtp.py` | Envio via `SMTP_SSL` com template `corpo.html` |
| `criptografia.py` | Fernet |
| `bot.py` | Automação Selenium do ERP Tiny (custos, contas a receber, GNRE, lotes). **Não está ligado a nenhum fluxo** |

---

## 9. Fluxos de negócio

Os parâmetros (TOP, natureza, vendedor, locais, categorias) vêm de `empresa`/`ecommerce`. Valores fixos no código:

- `CODCENCUS='0'`;
- `CODNAT '0'` na baixa de estoque;
- `CODVEND '1'` e `CODTIPVENDA '0'` na transferência;
- locais **101, 102, 911 e 500**;
- `CODEMP=31` e `CODTIPOPER=3229` em `sankhya/nota.py:107,115`.

Padrão de auditoria: `crud.log.criar(empresa_id, de, para, contexto)`, seguido de registros-filho por item e, ao final, `crud.log.atualizar(id, sucesso=not falhas)`.

### 9.1 Produto (`src/integrador/produto.py`)

Executado na ordem `receber_alteracoes` → `integrar_olist` → `integrar_snk`.

**a) `receber_alteracoes` — Olist → base local**
1. `olist.buscar_alteracoes()` (janela `OLIST_TEMPO_BUSCA_ALTER_PROD`, apenas `tipo='S'`).
2. Itens descartados:
   - sem SKU ou com `#K` (kit) → registra falha;
   - `sku == 1` (imposto) ou SKU não numérico → ignora.
3. Produto desconhecido com situação A/I → `crud.produto.criar`.
4. Se a alteração for mais recente que o registro local e não houver pendência → `pendencia=True`.

**b) `integrar_olist` — Sankhya → Olist** (fila `AD_OLISTRASTPRODUTO`)
- **Evento `I`** (`incluir_olist`): `parser.to_olist` → `olist.incluir` → `crud.produto.criar` → devolve `ID` ao Sankhya (`AD_OLISTPRODUTO`).
- **Evento `A`** (`atualizar_olist`): compara Sankhya × Olist; se houver divergência, chama `olist.atualizar`.
- Ao final, limpa a fila com `snk.excluir_alteracoes`.

**c) `integrar_snk` — Olist → Sankhya** (`crud.produto.buscar_pendencias`)
- Situação A/I → `update` (`ID`, `IDPRODPAI`); situação E → `delete` (`ATIVO='N'`).
- Grava `AD_OLISTPRODUTO` e volta para `pendencia=False`.

**Mapeamento de campos** (`src/parser/produto.py:to_olist`)

| Sankhya | Olist |
|---|---|
| `codprod` | `sku` (inclusão) |
| `nome` | `descricao` (inclusão) |
| `codvol` | `unidade` |
| `ncm` | `ncm` (validado `0000.00.00`) |
| `codespecst` | `codigoEspecificadorSubstituicaoTributaria` (CEST `00.000.00`) |
| `origprod` | `origem` |
| `referencia` | `gtin`, `tributacao.gtinEmbalagem` (dígito verificador validado) |
| `largura`/`altura`/`espessura` | `dimensoes.largura/altura/comprimento` |
| `pesoliq`/`pesobruto` | `dimensoes.pesoLiquido/pesoBruto` |
| `estmin`/`estmax` | `estoque.minimo/maximo` |
| `refforn` | `fornecedores[].codigoProdutoNoFornecedor` |

Na inclusão, o parser fixa `tipo='S'`, preços zerados, marca, categoria e fornecedor padrão da empresa, `estoque.controlar=True` e `seo.keywords=['produto']`. O payload de atualização é filtrado pelo template `src/json/produto.json["put"]`.

### 9.2 Estoque (`src/integrador/estoque.py`) — Sankhya → Olist

`atualizar_olist`:
1. Lê a fila `AD_OLISTRASTESTOQUE`, limitada a **300 produtos** por execução.
2. `sankhya.Estoque.buscar(lista)` (`buscar_estoque_atual.sql`).
3. Para cada produto:
   1. Busca o estoque no Olist e valida `id == idprod`.
   2. `calcular_variacao`:
      - produto sem estoque no Olist → tipo **`B`** (balanço) com o disponível, no depósito padrão;
      - sem `depositos` (kit) → ignora;
      - demais casos → `variacao = disponível_snk − saldo_olist`, tipo **`E`** se positiva ou **`S`** se negativa, com quantidade `|variacao|`.
   3. Se a variação for diferente de 0: `parser.to_olist` (template `estoque.json["post"]`) → `olist.enviar_saldo`.
   4. Remove o item da fila e registra em `log_estoque(qtdmov)`.
4. Uma exceção em qualquer item interrompe o lote inteiro.

### 9.3 Pedido (`src/integrador/pedido.py`)

#### a) Recebimento — Olist → base (`receber_novos`)

1. `consultar_cancelamentos`: pedidos cancelados no Olist recebem `dh_cancelamento`.
2. `olist.buscar_novos` (por `olist_situacao_busca_pedidos` ou `olist_dias_busca_pedidos`), seguido de `validar_existentes`.
3. `validar_loja`:
   - `id_loja` com 9 dígitos (Parfum/Funcionários) → filtra por `vendedor.id`;
   - caso contrário → filtra por `ecommerce.id`.
4. Para cada pedido, `receber(id)`:
   1. `validar_situacao`: cancelado → `dh_cancelamento`; incompleto → descarta.
   2. Desmembra kits (`#K`) via `olist.validar_kit`. O valor unitário é rateado igualmente entre os componentes.
   3. `cod_pedido = numeroPedidoEcommerce`, ou `"{id_loja}-{numeroPedido}"` quando não houver.
   4. `crud.pedido.criar(..., dados_pedido=<JSON>)`.
   5. Em caso de erro: marcador de erro e texto nas observações do Olist.

#### b) Separação — Olist → base (`src/integrador/separacao.py:receber`)

Lista as separações nas situações 1 e 4 e grava `pedido.id_separacao`, o que **libera o pedido para importação**.

#### c) Importação — base → Sankhya (`integrar_novos`)

**Modo lote** (`ecommerce.importa_pedido_lote = True`, `importar_agrupado`), um pedido de **ressuprimento** do e-commerce:

1. `unificar`: soma `qtdneg` por SKU. O SKU precisa casar com `^\d{8}`; caso contrário o pedido é descartado.
2. `sankhya.Estoque.buscar_saldo_por_local`.
3. `compara_saldos`:
   - `a_transferir = solicitado − saldo_ecommerce`;
   - arredondado para múltiplos de `AGRUPMIN` e limitado ao disponível;
   - consome os locais na ordem **911 (validade curta) → 102 (promoção) → 101 (matriz)**.
4. **Se houver itens a transferir:**
   1. Busca o valor de transferência (fallback 0.1).
   2. `parser.to_sankhya_pedido_venda`, com `CODEMP = snk_codemp_fornecedor`, `TOP = snk_top_pedido` e `CODLOCALORIG` = local de origem.
   3. `sankhya.Pedido.lancar`.
   4. `crud.pedido.atualizar(lista_ids, nunota, dh_importacao)`.
   5. Para cada pedido: grava o nunota nas observações do Olist e aplica o marcador “integrado” (ou “integrado teste” se `AMBIENTE=snd`).
5. **Se não houver itens a transferir:** `nunota = -1`, com `dh_importacao` e `dh_confirmacao` preenchidos, e marcador “integrado”.

**Modo unitário** (`importar_unico`):
1. Se o pedido já existe no Sankhya (`AD_MKP_ID`), apenas sincroniza a base.
2. Caso contrário:
   1. Busca o IBGE via ViaCEP e o `CODCID` no Sankhya.
   2. `parser.to_sankhya`.
   3. `lancar` e `atualizar_nunota` no Olist.

Mapeamento do cabeçalho em `parser.to_sankhya`:

| Olist | Sankhya |
|---|---|
| `id` | `AD_MKP_ID` |
| `numeroPedidoEcommerce` | `AD_MKP_CODPED` |
| `numeroPedido` | `AD_MKP_NUMPED` |
| `ecommerce.id` | `AD_MKP_ORIGEM` |
| `data` | `DTNEG` |
| `valorFrete − valorDesconto` | `VLRFRETE` |
| (empresa) | `CODEMP`, `CODNAT`, `CODPARC`, `CODTIPOPER=snk_top_pedido`, `CODTIPVENDA`, `CODVEND` |
| (fixo) | `TIPMOV='P'`, `CIF_FOB='C'`, `CODCENCUS='0'` |

Itens: `CODPROD` = primeiros 8 dígitos do SKU, `QTDNEG`, `VLRUNIT` (mínimo 0.01), `CODVOL`, `CODLOCALORIG = snk_codlocal_venda`.

#### d) Confirmação (`integrar_confirmacao`)

Agrupa os pedidos por `nunota`. Se `STATUSNOTA='L'`, apenas grava `dh_confirmacao`; caso contrário chama `ServicosNfeSP.confirmarNota` e depois grava `dh_confirmacao`.

#### e) Cancelamento e anulação

- `integrar_cancelamento(nunota)`: exclui o pedido no Sankhya → `crud.pedido.cancelar` → remove o nunota das observações no Olist.
- `anular_pedido_importado(nunota)`: bloqueado se já existe nota faturada. Caso contrário, exclui o pedido → `cancelar` → remove nunota e marcador “integrado”, com log `N`.

#### f) Relatório de separação (`reprocessar_relatorio_separacao`)

Recalcula `compara_saldos` para os pedidos não faturados e regrava `AD_OLISTRELPEDIDOS`.

### 9.4 Faturamento (`src/integrador/faturamento.py`)

Job `_faturar_completo`: `Pedido.consultar_cancelamentos` → `integrar_olist` → `integrar_snk`. Cada etapa é isolada, e os erros são acumulados por e-commerce.

#### a) `integrar_olist` — emissão da NF no Olist

Para cada pedido em `buscar_faturar`:
1. `Nota.gerar`: `POST /pedidos/{id}/gerar-nota-fiscal`; se a NF já existir, busca a existente. Grava `crud.nota.criar` com `parcelado = len(parcelas) > 1`.
2. `Nota.emitir`: grava `chave_acesso` e `dh_emissao`.
3. `separacao.separar` (situação 2).

#### b) `integrar_snk` — faturamento no Sankhya

Para cada `nunota` distinto:

- **Se `nunota != -1`:**
  1. Se ainda não faturado: `SelecaoDocumentoSP.faturar` (TOP `snk_top_transferencia`) → `atualizar_local` dos itens para `snk_codlocal_ecommerce`.
  2. Grava `dh_faturamento` e `nota.nunota`.
  3. `confirmarNota` da nota gerada → grava `dh_confirmacao`.
  4. Lança o **contas a pagar** da transferência no Olist (`Despesa.formatarPayloadLcto(dadosTransferencia)`: valor `vlrnota`, vencimento `dtneg + 30d`, categoria `olist_id_categoria_despesa_padrao`).
- **Se `nunota == -1`:** apenas grava `dh_faturamento` e `dh_confirmacao`.
- **Nos dois casos, `baixar_ecommerce`:**
  1. Unifica os itens das notas pendentes e valida as quantidades.
  2. `desmembrar_lotes_baixa_ecommerce`: distribui as quantidades entre os lotes com saldo no local do e-commerce.
  3. `to_sankhya_baixa_estoque_ecommerce` (`TOP = snk_top_baixa_estoque`, `TIPMOV='V'`, `CODLOCALORIG = snk_codlocal_ecommerce`, `CONTROLE` = lote) → `lancar` → `confirmar`.
  4. Grava `nota.baixa_estoque_ecommerce = True`.
- **Em falha:** `crud.pedido.informar_erro` + `notificar_erros` (e-mail).

#### c) Venda interna / transferência entre empresas (`realizar_venda_interna`)

1. Itens conferidos → saldo por lote → `compara_saldos` (respeita `AGRUPMIN` e `QTDMATRIZ`).
2. Busca o valor de transferência.
3. `parser.transferencia.to_sankhya`: `CODEMP = snk_codemp_fornecedor`, `CODEMPNEGOC = snk_codemp`, `TOP = snk_top_transferencia`, `CODNAT = snk_codnat_transferencia`, `TIPMOV='T'`.
4. `Transferencia.criar` → `confirmar`.

### 9.5 Nota (`src/integrador/nota.py`)

| Método | Ação |
|---|---|
| `gerar` | Gera a NF no Olist e cria o registro `nota` |
| `emitir` | Emite a NF no Olist; grava `chave_acesso` e `dh_emissao` |
| `receber_conta` | Busca o título a receber (`{serie}{numero:06}/01`) e grava `id_financeiro` |
| `integrar_cancelamento` | Exclui a nota no Sankhya e grava `dh_cancelamento` (somente no modo de importação unitária) |

### 9.6 Financeiro (`src/integrador/financeiro.py`, `src/scheduler/jobs/financeiro.py`)

Lança no Olist os títulos de **receita** (contas a receber) e **despesa** (taxas e frete do marketplace).

**Job `integrar(codemp, idLoja, dataFim, dias, processaShopee)`**, por empresa:

1. `NotaOlist.buscarData` → `Receita.processarNotas`, que calcula o `income_data` de cada nota:

   | Marketplace | `income_data` |
   |---|---|
   | Shopee | `{amount_paid, released_amount: 0, fee_shopee: 0}`, com valores reais preenchidos depois pela API Shopee |
   | Beleza na Web | `taxa = valorProdutos × BLZWEB_TAXA_COMISSAO + BLZWEB_TAXA_ENVIO`; `released_amount = valor − taxa`; `fee_blz` |
   | Demais | `{order_sn, amount_paid}` |

2. Para cada e-commerce:
   - **Se Shopee:**
     - `getIncomeDetail` → `atualizarDadosContaShopee` (`released_amount`, `fee_shopee`, `payout_time`).
     - Estornos (`released_amount ≤ 0` nos últimos 30 dias) → `getEscrowDetail` → `buscarEstornoShopee`.
   - `crud.nota.buscarPendenteLcto`. Para cada nota:
     - Shopee com `fee_shopee == 0` → pula (aguarda repasse).
     - Sem `id_financeiro` → lança a receita.
     - `id_loja` com 8 ou mais dígitos (Funcionários/Parfum) → `ignorarTaxa`.
     - Sem `id_financeiro_taxa` → lança a despesa da taxa (`olist_id_categoria_taxa_padrao`).
     - Com `fee_frete` e sem `id_financeiro_frete` → lança a despesa do frete (`olist_id_categoria_frete_padrao`).

**Vencimentos:**
- Receita Shopee: próxima quarta-feira.
- Demais receitas: dia 9 do mês seguinte.
- Despesa sem regra (marketplace que não é Shopee nem Beleza) → vencimento vazio, o que reprova a validação.

**Planilha** (`processar_titulos_planilha`): lançamentos manuais `receita`/`despesa` por `id_pedido`, recebidos pelo endpoint `/financeiro/processar`.

**Parser** (`src/parser/financeiro.py`):
- Receita: `data`, `dataVencimento`, `valor`, `numeroDocumento`, `contato.id`, `historico` (`"Ref. ao Pedido #X, NF nº Y"`), `categoria.id`, `formaRecebimento`, `ocorrencia='U'`.
- Despesa: equivalente, com `formaPagamento` e `quantidadeParcelas=1`.

### 9.7 Devolução (`src/integrador/devolucao.py`) — Olist → Sankhya

1. **`integrar_receber`:**
   1. `NotaOlist.buscar_devolucoes` (notas de entrada, últimos 5 dias).
   2. Para cada nota `tipo='E'`: extrai a chave referenciada (regex `\d{44}`) das observações e localiza a NF de venda por `chave_acesso`.
   3. `crud.devolucao.criar`, com log `D`.
2. **`integrar_devolucoes`:** para cada devolução não confirmada:
   1. `parser.to_sankhya_` monta a nota (`CODEMP = snk_codemp_fornecedor`, `TOP = snk_top_devolucao`, `TIPMOV='D'`, `SERIENOTA='2'`, `CODLOCALORIG = snk_codlocal_ecommerce`).
   2. `NotaSnk.devolver_sem_lote` (`CACSP.incluirNota`).
   3. Grava `nunota` e `dh_confirmacao`.
3. **`devolver_unico(numero)`:** devolução referenciada.
   1. `SelecaoDocumentoSP.faturar` com `QTDFAT` por sequência, limitado a `qtdneg − qtdentregue`.
   2. `alterar_observacao` → `confirmar`.
4. **`integrar_cancelamento`:** exclui a nota no Sankhya e grava `dh_cancelamento`.

---

## 10. API HTTP

**Sem autenticação.** Os corpos são JSON (Pydantic). Erros retornam HTTP 500 com `detail`.

| Método | Path | Body | Ação |
|---|---|---|---|
| GET | `/` | — | Título e versão |
| GET | `/ecommerces/buscar` | — | Lista as lojas ativas |
| GET | `/ecommerces/buscar/{codemp}` | — | Lojas de uma empresa |
| POST | `/ecommerces` | `EcommerceCreate` | Cadastra uma loja (nome recebe o sufixo da empresa) |
| GET | `/empresas/buscar` | — | Lista as empresas |
| GET | `/empresas/buscar/{codemp}` | — | Busca uma empresa |
| POST | `/empresas` | `EmpresaCreate` | Cadastra uma empresa |
| POST | `/produtos/integrar` | `{codemp}` | Sincronização de produtos |
| POST | `/estoque/integrar` | `{codemp}` | Envio de saldos |
| POST | `/pedidos/integrar-lote` | `{codemp}` | Recebe e importa os pedidos de todas as lojas da empresa |
| POST | `/pedidos/integrar-loja` | `{id_loja}` | Recebe e importa os pedidos de uma loja |
| POST | `/pedidos/receber` | `{id_loja, numero}` | Recebe um pedido específico |
| POST | `/pedidos/separacao/buscar` | `{codemp}` | Recebe as separações |
| POST | `/pedidos/faturar-lote` | `{codemp}` | Faturamento completo (Olist + Sankhya) |
| POST | `/pedidos/faturar-loja` | `{id_loja}` | Faturamento completo de uma loja |
| POST | `/pedidos/faturar/olist` | `{codemp}` | Somente emissão de NF no Olist |
| POST | `/pedidos/faturar-loja/olist` | `{id_loja}` | Idem, para uma loja |
| POST | `/pedidos/faturar/snk` | `{codemp}` | Somente faturamento no Sankhya |
| POST | `/pedidos/faturar/venda-interna` | `{codemp}` | Transferência entre empresas |
| POST | `/pedidos/anular` | `{codemp, nunota}` | Anula um pedido importado |
| POST | `/pedidos/relatorio` | `{codemp}` | Reprocessa o relatório de separação |
| POST | `/notas/cancelar` | `{id_loja, numero}` | Cancela a nota no Sankhya |
| POST | `/financeiro/integrar` | `{codemp?, idLoja?, dataFim?, dias=0, processaShopee=true}` | Lançamentos financeiros |
| POST | `/financeiro/processar` | `{codemp, idLoja, dtVcto, registros[]}` | Lançamentos a partir de planilha |
| POST | `/financeiro/processar-shopee` | `{codemp}` | Atualiza os repasses Shopee |
| POST | `/devolucoes/integrar` | `{codemp}` | Recebe e lança as devoluções |
| POST | `/devolucoes/devolver` | `{codemp, numero}` | Devolução referenciada de uma nota |
| POST | `/devolucoes/cancelar` | `{codemp, numero}` | Cancela uma devolução |

A documentação interativa fica em `/docs` (Swagger do FastAPI).

---

## 11. Agendador (scheduler)

`src/scheduler/scheduler.py`:
- `AsyncIOScheduler`, timezone `America/Sao_Paulo`.
- Jobs persistidos em `SQLAlchemyJobStore` (tabelas `apscheduler_*`, excluídas do autogenerate do Alembic).

| Job ID | Função | Trigger | Opções |
|---|---|---|---|
| `sincronizar_tudo` | `rotina_completa` | cron `hour="0-7,10-23"`, `minute="*/{MINUTOS_JOB_PADRAO}"` | `max_instances=1`, `coalesce=True` |
| `notificar_erros` | `rotina_notificacao` | cron `hour="12"` (12:00 diariamente) | idem |

**`rotina_completa`** executa, em série, para todas as empresas ativas:
1. `produtos.integrar_produtos()`: `receber_alteracoes` → `integrar_olist` → `integrar_snk`.
2. `estoque.integrar_estoque()`: `atualizar_olist`.
3. `pedidos.receber_pedido_lote()`: `Pedido.receber_novos` + `Separacao.receber`, por loja.

Cada rotina captura as próprias exceções, então a falha de uma não interrompe as seguintes.

**Não agendados** (somente via endpoint): importação e confirmação de pedidos, faturamento, financeiro e devoluções. As funções `rotina_lancamentos_financeiro`, `rotina_devolucoes` e `rotina_cache` existem, mas não são registradas.

**Comportamentos relevantes:**
- Um job só é criado se o ID ainda não existir no jobstore. **Alterar o trigger no código não altera um job já persistido**: é preciso removê-lo do banco.
- Existe um listener de retry (`<id>_retry` 5 minutos depois). Como as rotinas engolem as exceções, ele raramente é acionado.

---

## 12. Logging, auditoria e notificação

| Mecanismo | Implementação |
|---|---|
| Log em arquivo | `src/utils/log.py`: `./logs/AAAAMM.log`, nível INFO; `apscheduler` e `uvicorn.access` em WARNING. O arquivo é definido no primeiro `basicConfig`, então não troca na virada do mês enquanto o processo estiver vivo |
| Auditoria no banco | `log` + `log_produto` / `log_estoque` / `log_pedido` (ver [§5](#5-modelo-de-dados)) |
| Erro por pedido | `pedido.erro` / `pedido.erro_descricao` |
| Erro no Olist | Marcador “erro” + texto nas observações do pedido |
| E-mail diário | Job `notificar_erros` → `Email.notificar(empresa_id)`: falhas dos últimos `TEMPO_HISTORICO_MINUTOS` → `TO_DEFAULT` |
| E-mail pontual | `src/utils/notificar_erros.py` (destinatários em `src/utils/emails.json`, gitignored), usado em falhas de faturamento |
| Retenção | `crud.log.excluir_cache()` remove logs com mais de `DIAS_LIMPA_CACHE × 4` dias (não agendado) |

---

## 13. Segurança

- **Criptografia em repouso:** Fernet (AES-128-CBC + HMAC-SHA256), com chave em `PATH_FERNET_KEY`. Colunas criptografadas:
  - `empresa`: `client_secret`, `olist_admin_senha`, `snk_token`, `snk_appkey`, `snk_admin_senha`
  - `olist`: `token`, `refresh_token`, `id_token`
  - `sankhya`: `token`, `x_token`
  - `shopee`: `access_token`, `refresh_token`, `partner_key`
- ⚠️ Se o arquivo da chave não existir, uma **nova chave é gerada silenciosamente** (`criptografia.py:12-17`), o que torna ilegíveis todos os dados já criptografados. **Faça backup de `keys/.fernet.key`.**
- ⚠️ A API não tem autenticação e usa CORS `*` com `allow_credentials=True`. Deve ser exposta somente em rede interna.
- ⚠️ Os SQLs do Sankhya são montados por interpolação de strings (`format_map`), sem parâmetros vinculados.
- `keys/.env`, `*.key`, `*.sql` e `emails.json` estão no `.gitignore`.

---

## 14. Scripts utilitários

Executar a partir da raiz (`python -m scripts.<nome>`).

| Script | Finalidade |
|---|---|
| `checar_pedidos.py` | Lista os pedidos do Olist |
| `create_contas_a_pagar.py` | Correção pontual: recria no Olist os contas a pagar de transferências (`CODTIPOPER=1419`, período fixo no código) |
| `get_contas_a_pagar.py` | Consulta um título a pagar por ID fixo |
| `testar_auth.py` | Testa a obtenção de token no Sankhya |
| `database/dev/populate_db.py` | Popula empresa, ecommerce, sankhya e produtos a partir de CSVs |

---

## 15. Limitações conhecidas e débitos técnicos

Itens identificados na leitura do código. Os marcados com ✔ foram conferidos diretamente.

### Bugs

| # | Local | Descrição |
|---|---|---|
| 1 ✔ | `src/integrador/financeiro.py:764` | `Despesa.ignorarTaxa` lança exceção quando o `update` **funciona** (condição invertida), interrompendo o financeiro da empresa |
| 2 ✔ | `routers/notas.py:18` | `asyncio.run()` dentro de handler `async`: `RuntimeError`, o endpoint sempre falha |
| 3 ✔ | `src/sql/buscar_estoque_atual.sql` × `src/sankhya/estoque.py:56-65` | Placeholders em maiúsculas no SQL e chaves em minúsculas no `format_map`: `KeyError` |
| 4 ✔ | `src/services/smtp.py:117` | Argumentos de `format` fora de ordem em relação a `corpo.html` (corpo e minutos trocados) |
| 5 | `src/integrador/pedido.py:1131-1136` | `pedidos_confirmar` não é definido quando `importa_pedido_lote=False` (`NameError`) |
| 6 | `src/integrador/pedido.py:209,248,255` | `pedido_olist` não é definido em `receber` quando `dados_pedido` é passado como argumento |
| 7 | `src/integrador/pedido.py:412-424` | Detecção de kit: só `#K` é tratado; o ramo `elif "-"` é inalcançável |
| 8 | `src/parser/pedido.py:57,66,109,157`, `src/parser/devolucao.py:77-79` | Vírgula final transforma valores em tupla |
| 9 | `src/integrador/faturamento.py:208,216` | Coroutine `to_sankhya` chamada sem `await` |
| 10 | `src/integrador/faturamento.py:267-270` | `.get()` chamado sobre `bool` |
| 11 | `src/parser/transferencia.py:83` | Variável `item` indefinida |
| 12 | `src/integrador/produto.py:292-298` | `tipo_atualizacao` indefinido para situação desconhecida |
| 13 | `src/integrador/devolucao.py:63-64` | `.group(0)` sobre `None` quando não há chave nas observações |
| 14 | `src/parser/devolucao.py:40` | `continue` no laço interno: item adicionado em duplicidade |
| 15 | `src/utils/busca_paginada.py:30` | `&offset=` acumulado na URL a cada página |
| 16 | `paginar_snk`, `sankhya.Produto.buscar_alteracoes` | Laço infinito com HTTP 200 e `status != '1'` |
| 17 | `routers/financeiro.py`, `routers/devolucoes.py` | `if not <dict>` nunca dispara: falhas são retornadas como `true` |
| 18 | `src/scheduler/jobs/limpar_cache.py` | Chama `excluir_cache` inexistente em `crud.sankhya`/`crud.olist` |
| 19 | `database/crud/nota.py:39` | Acesso a `.numero` em retorno `bool` |
| 20 | `database/crud/sankhya.py:72` | Filtro por `Sankhya.id`, mas a PK é `app_id` |
| 21 | `src/sankhya/financeiro.py:89` | `self.campos` não definido |

### Débitos arquiteturais

- **I/O bloqueante em código assíncrono:** `requests` e `time.sleep` dentro de corrotinas bloqueiam o event loop, incluindo durante a execução do scheduler.
- **Sem resiliência HTTP:** sem timeout, retry ou backoff. Uma requisição travada bloqueia o processo indefinidamente.
- **Locks de token por processo:** com mais de um worker ou processo, pode haver renovações concorrentes.
- **Primeiro login Olist via Selenium/Firefox:** depende de navegador disponível no servidor.
- **Valores fixos no código:** locais 101/102/911/500, `CODEMP=31`/`CODTIPOPER=3229` em `sankhya/nota.py`, IDs de empresas no `bot.py`, prefixo Beleza na Web em `financeiro.py:126`.
- **Retornos heterogêneos:** `False`, `0`, `{}`, `[]`, `set` e tuplas para indicar erro dificultam o tratamento uniforme.
- **Templates não utilizados:** `src/json/pedido.json`, `nota.json` e `financeiro.json`.
- **Código sem uso:** `src/services/bot.py`, `src/parser/conferencia.py`, `src/sankhya/financeiro.py`, `parser.pedido.to_sankhya_lote`.
- **Sem testes automatizados**, apesar de `pytest` constar nas dependências.
- **Schema de produção fora do Alembic** (ver [§5](#5-modelo-de-dados)).
