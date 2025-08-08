from pydantic import BaseModel, Field, field_validator
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo

TZ_SAO_PAULO = ZoneInfo("America/Sao_Paulo")

def get_now_sao_paulo() -> datetime:
    """Returns the current time in 'America/Sao_Paulo' timezone."""
    return datetime.now(TZ_SAO_PAULO)

class TokenOlistBase(BaseModel):

    dh_solicitacao: datetime = Field(default_factory=get_now_sao_paulo)
    token_criptografado: str
    dh_expiracao_token: datetime
    refresh_token_criptografado: str
    dh_expiracao_refresh_token: datetime
    id_token_criptografado: str

    @field_validator('dh_expiracao_token', 'dh_expiracao_refresh_token', mode='before')
    @classmethod
    def ensure_timezone_sao_paulo(cls, v: datetime) -> datetime:
        """Converte a data/hora recebida para o timezone America/Sao_Paulo."""
        if v.tzinfo is None:
            # Assume que é hora local ingênua e converte corretamente
            return v.replace(tzinfo=TZ_SAO_PAULO)
        else:
            # Converte para o fuso correto
            return v.astimezone(TZ_SAO_PAULO)

    class Config:
        from_attributes = True
        orm_mode = True

class TokenOlist(TokenOlistBase):
    id: int

TokensOlist = List[TokenOlistBase]
