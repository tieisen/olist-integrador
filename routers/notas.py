from fastapi import APIRouter, HTTPException, status
from src.integrador.nota import Nota
import asyncio

router = APIRouter()
nota = Nota()

@router.get("/")
def default():
    return {"message": "Notas"}

@router.get("/baixar-contas")
def baixar_contas():
    """
    Baixa contas a receber que estão pendentes.
    """
    if not asyncio.run(nota.baixar_financeiro()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao baixar contas a receber")
    return True