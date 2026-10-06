# CLAUDE.md

Integração entre o ERP Sankhya (SNK) e a Olist (Tiny API v3): produtos, estoque, pedidos, notas, financeiro e devoluções. Os usuários operam direto pelo Sankhya; o front-end (tieisen/olist-painel) não é utilizado.

**Especificação técnica completa: [docs/SPEC.md](docs/SPEC.md).** Consulte-a antes de alterar qualquer fluxo de negócio, e atualize-a quando mudar comportamento, endpoints, jobs ou modelo de dados.

## Convenções

- Código, identificadores, logs e mensagens de commit em **português**.
- Lint/format com Ruff (line-length 100) via pre-commit; não há testes automatizados.
- Rode tudo a partir da raiz do repositório: `keys/.env` é carregado por caminho relativo (modelo em `keys/example.env`; variáveis em [SPEC §4](docs/SPEC.md#4-configuração-variáveis-de-ambiente)).

## Comandos

```bash
pre-commit install                          # ruff --fix + ruff-format no commit
docker compose up -d                        # PostgreSQL local de homologação
alembic upgrade head                        # aplica as migrations
alembic revision --autogenerate -m "..."    # após alterar database/models.py; revise antes de aplicar
alembic stamp head                          # marca o banco como atualizado sem executar DDL
python -m database                          # cria o banco de DB_NAME + create_all (só dev)
python .                                    # sobe a API (HOST/PORT do .env)
```

## Produção e migrations

- **Nunca** rode `python -m database` nem `alembic revision --autogenerate` apontando para produção.
- O schema da produção foi alterado manualmente e **não está versionado no Alembic**. Não rode `alembic upgrade` lá: antes compare o schema com `database/models.py` e então faça `alembic stamp head`.
- Nunca edite ou apague uma migration já commitada; crie uma nova.
- Alterou `database/models.py`? Atualize também `database/schemas.py` (hoje defasado) e o [SPEC §5](docs/SPEC.md#5-modelo-de-dados).

## Arquitetura

- Fluxo: `routers/` → `src/scheduler/jobs/` → classes de `src/integrador/` → clientes `src/olist/` e `src/sankhya/` + `src/parser/` + `database/crud/`. Jobs agendados e endpoints manuais executam o **mesmo** código de `jobs/`. Detalhes: [SPEC §3](docs/SPEC.md#3-arquitetura).
- Multi-tenant: uma `empresa` (`snk_codemp`) tem vários `ecommerce` (`id_loja`). TOPs, naturezas, locais e credenciais de cada tenant vêm do **banco**, não do env; os jobs iteram sobre todos eles. Não fixe esses valores no código.
- Decoradores de `src/utils/decorador.py` (`carrega_dados_empresa`, `carrega_dados_ecommerce`, `contexto`, ...) exigem que a classe tenha `self.contexto`, `self.id_loja`, `self.codemp` e `self.empresa_id` ([SPEC §7](docs/SPEC.md#7-decoradores)).
- Estado do pedido e da nota é inferido por colunas de data/ID preenchidas, não por um campo de status ([SPEC §5](docs/SPEC.md#5-modelo-de-dados)).
- Fluxos de negócio por domínio (produto, estoque, pedido, faturamento, financeiro, devolução): [SPEC §9](docs/SPEC.md#9-fluxos-de-negócio).

## Armadilhas

- **Scheduler:** só `rotina_completa` (produtos → estoque → recebimento de pedidos) e `notificar_erros` estão agendados. Importação, faturamento, financeiro e devoluções rodam apenas por endpoint. Os jobs ficam persistidos no banco: mudar o trigger no código **não** altera um job já gravado ([SPEC §11](docs/SPEC.md#11-agendador-scheduler)).
- **`src/sql/*.sql` não está no git** (`.gitignore: *.sql`). Os scripts são lidos via variáveis `SANKHYA_PATH_SCRIPT_*` e interpolados com `format_map`: mantenha as chaves do SQL idênticas às passadas no código (maiúsculas/minúsculas contam).
- **`keys/.fernet.key`:** se faltar, uma chave nova é gerada em silêncio e os tokens/credenciais já salvos ficam ilegíveis. Nunca apague nem regenere.
- **I/O síncrono em código `async`:** `requests` e `time.sleep` bloqueiam o event loop, e as chamadas não têm timeout nem retry. Não presuma concorrência real.
- **Convenção de retorno:** CRUD e clientes devolvem `False`/`0`/`{}`/`[]` em erro em vez de lançar exceção, e os endpoints de `financeiro` e `devolucoes` tratam dict como sucesso. Ao tratar o retorno, verifique o campo `status`/`sucesso`, não só a veracidade do valor.
- **API sem autenticação** e com CORS aberto: não exponha fora de rede interna.
- `AMBIENTE=snd` ativa o comportamento de sandbox (sem escrita de observações no Olist, marcadores de teste).

Bugs conhecidos e débitos técnicos: [SPEC §15](docs/SPEC.md#15-limitações-conhecidas-e-débitos-técnicos). Verifique essa lista antes de depurar um fluxo.
