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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao receber alterações nos cadastros de produtos do Olist")
    if not asyncio.run(produto.integrar_olist()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao enviar produtos para Olist")        
    if not asyncio.run(produto.integrar_snk()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao atualizar produtos no Sankhya")        
    return True