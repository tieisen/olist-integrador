from fastapi import APIRouter, HTTPException, status
from src.integrador.nota import Nota
import asyncio
from pydantic import BaseModel

router = APIRouter()

class NotaModel(BaseModel):
    id_loja:int
    numero:int

@router.post("/cancelar")
async def cancelar_nota(nota:NotaModel) -> bool:
    """
    Cancela nota
    """
    _nota = Nota(id_loja=nota.id_loja)
    if not asyncio.run(_nota.integrar_cancelamento(numero=nota.numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao cancelar nota")
    return True