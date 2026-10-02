# populate_db.py

## Propósito

Script auxiliar para popular um banco (dev/homologação) com os dados iniciais de `empresa` e `ecommerce`, a partir de arquivos CSV. É o passo recomendado logo após criar o schema (`alembic upgrade head`), para não precisar cadastrar empresas/ecommerces manualmente pela API antes de começar a testar a integração.

## Como funciona

1. Lê os arquivos CSV em `database/dev/data/`:
   - `tabela-empresa.csv` → tabela `empresa`
   - `tabela-ecommerce.csv` → tabela `ecommerce`
2. Para cada linha, converte os valores (que o `csv.DictReader` sempre lê como `str`) para o tipo da coluna correspondente no model (`database/models.py`), via `cast_row()`:
   - colunas `Boolean` → `"True"`/`"False"` viram `bool`
   - colunas `Integer` → convertidas com `int(...)`
   - colunas `DateTime` → convertidas com `datetime.fromisoformat(...)`
   - células vazias (`""`) ou com o texto `"NULL"` viram `None`

   Isso é necessário porque o driver assíncrono (`asyncpg`) não faz esse cast automaticamente — diferente do `psycopg2`, ele rejeita, por exemplo, passar a string `"True"` para uma coluna `Boolean`.
3. Antes de inserir, verifica se o registro já existe, para o script poder ser executado mais de uma vez sem duplicar dados:
   - `empresa`: procura por `snk_codemp`
   - `ecommerce`: procura pela combinação `id_loja` + `empresa_id`
4. Insere e dá `commit` linha a linha. Se uma linha falhar (dado inválido, FK inexistente etc.), o erro é logado no console com o `id` da linha e a exceção original, a sessão é revertida (`rollback`) e o script segue para a próxima linha — uma linha com problema não interrompe o restante do arquivo.
5. `populate_db()` roda `populate_empresa()` antes de `populate_ecommerce()`, pois cada linha de `ecommerce` referencia uma `empresa` via `empresa_id` (chave estrangeira).

## Dados que ele precisa

### Configuração

Mesmas variáveis de ambiente do restante do projeto, em `keys/.env` (ver `keys/example.env` e o [README principal](../../README.md#ambiente-de-homologação-com-docker)):

- `POSTGRES_URL`
- `DB_NAME`

O banco e as tabelas (`empresa`, `ecommerce`) precisam já existir — rode `python -m database` ou `alembic upgrade head` antes.

### Arquivos CSV

Devem estar em `database/dev/data/`, com cabeçalho igual ao nome das colunas do model correspondente (a ordem das colunas não importa, o mapeamento é feito pelo nome do cabeçalho):

**`tabela-empresa.csv`** — colunas da tabela `empresa`:
`id`, `dh_criacao`, `dh_atualizacao`, `ativo`, `snk_codemp`, `nome`, `cnpj`, `serie_nfe`, `client_id`, `client_secret`, `olist_admin_email`, `olist_admin_senha`, `olist_id_fornecedor_padrao`, `olist_id_deposito_padrao`, `olist_dias_busca_pedidos`, `olist_situacao_busca_pedidos`, `olist_id_conta_destino`, `olist_id_categoria_padrao`, `olist_id_categoria_despesa_padrao`, `olist_id_categoria_taxa_padrao`, `olist_id_categoria_frete_padrao`, `olist_id_marca_padrao`, `snk_token`, `snk_appkey`, `snk_admin_email`, `snk_admin_senha`, `snk_timeout_token_min`, `snk_top_pedido`, `snk_top_venda`, `snk_top_transferencia`, `snk_top_devolucao`, `snk_top_baixa_estoque`, `snk_codvend`, `snk_codcencus`, `snk_codnat`, `snk_codnat_transferencia`, `snk_codtipvenda`, `snk_codusu_integracao`, `snk_codtab_transf`, `snk_codlocal_estoque`, `snk_codlocal_venda`, `snk_codlocal_ecommerce`, `snk_codparc`, `snk_codemp_fornecedor`, `snk_obs_transferencia`

**`tabela-ecommerce.csv`** — colunas da tabela `ecommerce`:
`id`, `id_loja`, `nome`, `id_fornecedor_olist`, `id_conta_destino`, `id_categoria_financeiro`, `id_forma_pgto_padrao`, `id_forma_rec_padrao`, `id_deposito`, `dh_criacao`, `dh_atualizacao`, `importa_pedido_lote`, `limite_pedido_lote`, `ativo`, `empresa_id`

> `empresa_id` precisa corresponder a um `id` já cadastrado (ou que será cadastrado antes, via `tabela-empresa.csv`) na tabela `empresa`.

## Como executar

A partir da raiz do projeto, com o ambiente virtual ativado:

```bash
python -m database.dev.populate_db
```

O script pode ser executado quantas vezes forem necessárias — linhas já cadastradas são identificadas e puladas (mensagem `"Empresa já existe"` / `"ecommerce já existe"`).
