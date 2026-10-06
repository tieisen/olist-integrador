# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Visão geral

Integração entre o ERP Sankhya (SNK) e a Olist (Tiny): produtos, estoque, pedidos, notas, financeiro e devoluções. Código, identificadores, logs e mensagens de commit são em português — mantenha essa convenção. O front-end (tieisen/olist-painel) não é utilizado; os usuários operam direto pelo Sankhya.

## Comandos

Execute sempre a partir da raiz do repositório (`keys/.env` é carregado por caminho relativo; modelo em `keys/example.env`).

```bash
pre-commit install                          # ruff --fix + ruff-format no commit
docker compose up -d                        # PostgreSQL local de homologação
alembic upgrade head                        # aplica as migrations
alembic revision --autogenerate -m "..."    # após alterar database/models.py; revise antes de aplicar
alembic stamp head                          # marca o banco como atualizado sem executar DDL
python -m database                          # cria o banco de DB_NAME + create_all (só dev)
```

Não há testes automatizados.

## Produção e migrations

- **Nunca** rode `python -m database` nem `alembic revision --autogenerate` apontando para produção.
- O schema da produção foi alterado manualmente e **não está versionado no Alembic**. Não rode `alembic upgrade` lá: antes é preciso comparar o schema com `database/models.py` e então fazer `alembic stamp head`.
- Nunca edite ou apague uma migration já commitada; crie uma nova.

## Arquitetura

- Fluxo: `routers/` → `src/scheduler/jobs/` → classes de `src/integrador/` → clientes `src/olist/` e `src/sankhya/` + `src/parser/` (conversão entre formatos) + `database/crud/`. Os jobs agendados (APScheduler, `SCHEDULER_ENABLE`) e os endpoints manuais usam o mesmo código dos jobs.
- Multi-tenant: uma `empresa` (Sankhya, `snk_codemp`) tem vários `ecommerce` (loja Olist, `id_loja`). Os dados de cada tenant vêm do banco, não do env; os jobs iteram sobre todos eles.
- Decoradores de `src/utils/decorador.py` (`carrega_dados_empresa`, `carrega_dados_ecommerce`, `contexto`, ...) exigem que a classe tenha `self.contexto`, `self.id_loja`, `self.codemp` e `self.empresa_id`.
