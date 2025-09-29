from fastapi import APIRouter, HTTPException, status
from src.integrador.produto import Produto
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
    produto = Produto(codemp=codemp)    
    if not asyncio.run(produto.receber_alteracoes()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao receber alterações nos cadastros de produtos do Olist")
    if not asyncio.run(produto.integrar_olist()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao enviar produtos para Olist")        
    if not asyncio.run(produto.integrar_snk()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao atualizar produtos no Sankhya")        
    return True