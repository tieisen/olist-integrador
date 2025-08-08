from fastapi import APIRouter
from src.integrador.produto import Produto
import asyncio

router = APIRouter()

@router.get("/")
def default():
    return {"message": "Produtos"}

@router.get("/buscar")
def buscar_produtos_olist():
    produto = Produto()
    asyncio.run(produto.recebe_alteracoes_pendentes())
    return True

@router.get("/integrar")
def integrar_produtos_olist():
    produto = Produto()
    asyncio.run(produto.atualiza_olist_rotina())
    asyncio.run(produto.atualiza_snk_rotina())
    return True