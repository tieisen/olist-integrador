from fastapi import APIRouter, HTTPException, status
from src.integrador.nota import Nota
import asyncio

router = APIRouter()
nota = Nota()

@router.get("/")
def default():
    return {"message": "Notas"}

@router.get("/{id}/baixar-contas")
def baixar_contas(id:int):
    """
    Baixa contas a receber que estão pendentes.
    """
    if not asyncio.run(nota.baixar_conta(id_nota=id)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao baixar contas a receber")
    return True

@router.get("/cancelar/{numero}")
def cancelar_nota(numero:int):
    """
    Cancela nota de devolução
    """
    if not asyncio.run(nota.integrar_cancelamento(numero=numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao cancelar nota")
    return True