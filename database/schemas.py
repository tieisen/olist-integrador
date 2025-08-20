from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo

def get_now_sao_paulo() -> datetime:
    """Returns the current time in 'America/Sao_Paulo' timezone."""
    return datetime.now(ZoneInfo("America/Sao_Paulo"))

############## LOG ESTOQUE
class LogEstoqueBase(BaseModel):
    log_id: int | None = None
    codprod: int
    idprod: int
    dh_atualizacao: datetime = Field(default_factory=get_now_sao_paulo)
    qtdmov: int
    status_estoque: bool | None = None
    status_lotes: bool | None = None
    obs: str | None = None

    class Config:
        from_attributes = True

class LogEstoque(LogEstoqueBase):
    id: int

LogsEstoque = List[LogEstoqueBase]

############## LOG PEDIDO
class LogPedidoBase(BaseModel):
    log_id: int | None = None
    id_loja: int
    id_pedido: int
    pedido_ecommerce: str
    id_nota: int | None = None
    nunota_pedido: int | None = None
    nunota_nota: int | None = None
    dh_atualizacao: datetime = Field(default_factory=get_now_sao_paulo)
    evento: str
    status: bool | None = None
    obs: str | None = None

    class Config:
        from_attributes = True

class LogPedido(LogPedidoBase):
    id: int

LogsPedido = List[LogPedidoBase]

############## LOG PRODUTO
class LogProdutoBase(BaseModel):
    log_id: int | None = None
    codprod: int
    idprod: int
    dh_atualizacao: datetime = Field(default_factory=get_now_sao_paulo)
    campo: str | None = None
    valor_old: str | None = None
    valor_new: str | None = None
    sucesso: bool | None = None
    obs: str | None = None

    class Config:
        from_attributes = True

class LogProduto(LogProdutoBase):
    id: int

LogsProduto = List[LogProdutoBase]

############## LOG
class LogBase(BaseModel):
    dh_execucao: datetime = Field(default_factory=get_now_sao_paulo)
    contexto: str | None = None
    de: str | None = None
    para: str | None = None
    sucesso: bool | None = None

    class Config:
        from_attributes = True

class Log(LogBase):
    id: int

Logs = List[LogBase]

############## PRODUTO
class ProdutoBase(BaseModel):
    cod_snk: int
    cod_olist: int    
    dh_cadastro: datetime | None = None
    dh_atualizado: datetime | None = None
    pendencia: bool | None = None

    class Config:
        from_attributes = True

class Produto(ProdutoBase):
    id: int

Produtos = List[ProdutoBase]

############## TOKEN OLIST
class TokenOlistBase(BaseModel):
    dh_solicitacao: datetime = Field(default_factory=get_now_sao_paulo)
    token_criptografado: str
    dh_expiracao_token: datetime
    refresh_token_criptografado: str
    dh_expiracao_refresh_token: datetime
    id_token_criptografado: str

    class Config:
        from_attributes = True

class TokenOlist(TokenOlistBase):
    id: int

TokensOlist = List[TokenOlistBase]

############## TOKEN SANKHYA
class TokenSankhyaBase(BaseModel):
    dh_solicitacao: datetime = Field(default_factory=get_now_sao_paulo)
    token_criptografado: str
    dh_expiracao_token: datetime

    class Config:
        from_attributes = True

class TokenSankhya(TokenSankhyaBase):
    id: int

TokensSankhya = List[TokenSankhyaBase]

############## VENDA
class VendaBase(BaseModel):
    id_loja: int
    dh_pedido: datetime = Field(default_factory=get_now_sao_paulo)
    id_pedido: int
    cod_pedido: str
    num_pedido: int
    id_separacao: int | None = None
    num_nota: int | None = None
    id_nota: int | None = None
    dh_nota_emissao: datetime | None = None
    id_financeiro: int | None = None
    dh_baixa_financeiro: datetime | None = None
    dh_cancelamento_pedido: datetime | None = None
    dh_cancelamento_nota: datetime | None = None
    nunota_pedido: int | None = None
    dh_importacao_pedido_snk: datetime | None = None
    dh_confirmacao_pedido_snk: datetime | None = None
    dh_faturamento_snk: datetime | None = None
    nunota_nota: int | None = None
    dh_confirmacao_nota_snk: datetime | None = None

    class Config:
        from_attributes = True

class Venda(VendaBase):
    id: int

Vendas = List[VendaBase]