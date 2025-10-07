from fastapi import APIRouter, HTTPException, status
from src.scheduler.jobs.produtos import integrar_produtos
import asyncio

router = APIRouter()

@router.get("/")
def default():
    return {"message": "Produtos"}

@router.get("/{codemp}/integrar")
def integrar_produtos(codemp:int):
    """
    Cria o cadastro de produtos novos no Olist.
    Atualiza no Olist os dados dos produtos que tiveram alteração no cadastro do Sankhya.
    Atualiza no Sankhya os dados dos produtos que tiveram alteração no cadastro do Olist.
    """
    if not asyncio.run(integrar_produtos(codemp=codemp)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro integrar produtos")
    return True