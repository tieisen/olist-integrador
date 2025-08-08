from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo

def get_now_sao_paulo() -> datetime:
    """Returns the current time in 'America/Sao_Paulo' timezone."""
    return datetime.now(ZoneInfo("America/Sao_Paulo"))

class VendaBase(BaseModel):
    id_loja: int
    dh_pedido: datetime = Field(default_factory=get_now_sao_paulo)
    id_pedido: int
    cod_pedido: str
    num_pedido: int
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
        orm_mode = True

class Venda(VendaBase):
    id: int

Vendas = List[VendaBase]