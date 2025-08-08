from fastapi import APIRouter
from src.integrador.estoque import Estoque
import asyncio

router = APIRouter()

@router.get("/")
def default():
    return {"message": "Estoque"}

@router.get("/integrar")
def integrar_produtos_olist():
    estoque = Estoque()
    asyncio.run(estoque.atualiza_olist())
    return True