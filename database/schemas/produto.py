from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo

def get_now_sao_paulo() -> datetime:
    """Returns the current time in 'America/Sao_Paulo' timezone."""
    return datetime.now(ZoneInfo("America/Sao_Paulo"))

class ProdutoBase(BaseModel):
    cod_snk: int
    cod_olist: int    
    dh_cadastro: datetime | None = None
    dh_atualizado: datetime | None = None
    pendencia: bool | None = None

    class Config:
        from_attributes = True
        orm_mode = True

class Produto(ProdutoBase):
    id: int

Produtos = List[ProdutoBase]