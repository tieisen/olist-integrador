from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo

def get_now_sao_paulo() -> datetime:
    """Returns the current time in 'America/Sao_Paulo' timezone."""
    return datetime.now(ZoneInfo("America/Sao_Paulo"))

class LogBase(BaseModel):
    dh_execucao: datetime = Field(default_factory=get_now_sao_paulo)
    contexto: str | None = None
    de: str | None = None
    para: str | None = None
    sucesso: bool | None = None

    class Config:
        from_attributes = True
        orm_mode = True

class Log(LogBase):
    id: int

Logs = List[LogBase]