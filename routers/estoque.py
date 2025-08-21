from fastapi import APIRouter, HTTPException, status
from src.integrador.estoque import Estoque
import asyncio

router = APIRouter()
estoque = Estoque()

@router.get("/")
def default():
    return {"message": "Estoque"}

@router.get("/integrar")
def integrar_estoque():
    """
    Atualiza o saldo de estoque no Olist dos produtos que tiveram alteração no saldo de estoque do Sankhya.
    """
    if not asyncio.run(estoque.atualizar_olist()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar estoque")
    return True