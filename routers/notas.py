from fastapi import APIRouter, HTTPException, status
from src.integrador.nota import Nota
import asyncio
from pydantic import BaseModel

router = APIRouter()

class NotaModel(BaseModel):
    id_loja:int
    numero:int

class ContaModel(BaseModel):
    id_loja:int
    id:int

@router.post("/baixar-contas")
async def baixar_contas(nota:ContaModel) -> bool:
    """
    Baixa contas a receber que estão pendentes.
    """
    _nota = Nota(id_loja=nota.id_loja)
    if not asyncio.run(_nota.baixar_conta(id_nota=nota.id)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao baixar contas a receber")
    return True

@router.post("/cancelar")
async def cancelar_nota(nota:NotaModel) -> bool:
    """
    Cancela nota de devolução
    """
    _nota = Nota(id_loja=nota.id_loja)
    if not asyncio.run(_nota.integrar_cancelamento(numero=nota.numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao cancelar nota")
    return True