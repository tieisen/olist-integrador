from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo

def get_now_sao_paulo() -> datetime:
    """Returns the current time in 'America/Sao_Paulo' timezone."""
    return datetime.now(ZoneInfo("America/Sao_Paulo"))

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
        orm_mode = True

class LogProduto(LogProdutoBase):
    id: int

LogsProduto = List[LogProdutoBase]