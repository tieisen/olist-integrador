from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo

def get_now_sao_paulo() -> datetime:
    """Returns the current time in 'America/Sao_Paulo' timezone."""
    return datetime.now(ZoneInfo("America/Sao_Paulo"))

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
        orm_mode = True

class LogPedido(LogPedidoBase):
    id: int

LogsPedido = List[LogPedidoBase]