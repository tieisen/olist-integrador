from fastapi import APIRouter, HTTPException, status
from src.integrador.produto import Produto
import asyncio

router = APIRouter()
produto = Produto()

@router.get("/")
def default():
    return {"message": "Produtos"}

@router.get("/integrar")
def integrar_produtos():
    """
    Cria o cadastro de produtos novos no Olist.
    Atualiza no Olist os dados dos produtos que tiveram alteração no cadastro do Sankhya.
    Atualiza no Sankhya os dados dos produtos que tiveram alteração no cadastro do Olist.
    """
    if not asyncio.run(produto.receber_alteracoes()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar produtos")
    if not asyncio.run(produto.atualizar_olist()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar produtos")        
    if not asyncio.run(produto.atualizar_snk()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar produtos")        
    return True