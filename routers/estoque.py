from fastapi import APIRouter, HTTPException, status
from src.integrador.estoque import Estoque
import asyncio

router = APIRouter()

@router.get("/")
def default():
    return {"message": "Estoque"}

@router.get("{codemp}/integrar")
def integrar_estoque(codemp:int):
    """
    Atualiza o saldo de estoque no Olist dos produtos que tiveram alteração no saldo de estoque do Sankhya.
    """
    estoque = Estoque(codemp=codemp)
    if not asyncio.run(estoque.atualizar_olist()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar estoque")
    return True