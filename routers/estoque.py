from fastapi import APIRouter, HTTPException, status
from src.scheduler.jobs.estoque import integrar_estoque
import asyncio

router = APIRouter()

@router.get("")
def default():
    return {"message": "Estoque"}

@router.get("/{codemp}/integrar")
def integrar(codemp:int):
    """
    Atualiza o saldo de estoque no Olist dos produtos que tiveram alteração no saldo de estoque do Sankhya.
    """
    if not asyncio.run(integrar_estoque(codemp=codemp)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar estoque")
    return True