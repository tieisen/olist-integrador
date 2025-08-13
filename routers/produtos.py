from fastapi import APIRouter
from src.integrador.produto import Produto
import asyncio

router = APIRouter()
produto = Produto()

@router.get("/")
def default():
    return {"message": "Produtos"}

@router.get("/buscar")
def buscar_produtos_olist():    
    return asyncio.run(produto.receber_alteracoes())

@router.get("/integrar")
def integrar_produtos_olist():
    ack_olist = asyncio.run(produto.atualizar_olist())
    ack_snk = asyncio.run(produto.atualizar_snk())
    return all([ack_olist, ack_snk])