from fastapi import APIRouter, HTTPException, status
from src.integrador.nota import Nota
import asyncio

router = APIRouter()

@router.get("")
def default():
    return {"message": "Notas"}

@router.get("/{id_loja}/{id}/baixar-contas")
def baixar_contas(id_loja:int,id:int):
    """
    Baixa contas a receber que estão pendentes.
    """
    nota = Nota(id_loja=id_loja)
    if not asyncio.run(nota.baixar_conta(id_nota=id)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao baixar contas a receber")
    return True

@router.get("/{id_loja}/cancelar/{numero}")
def cancelar_nota(id_loja:int,numero:int):
    """
    Cancela nota de devolução
    """
    nota = Nota(id_loja=id_loja)
    if not asyncio.run(nota.integrar_cancelamento(numero=numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao cancelar nota")
    return True