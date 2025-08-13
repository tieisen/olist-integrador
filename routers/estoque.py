from fastapi import APIRouter
from src.integrador.estoque import Estoque
import asyncio

router = APIRouter()
estoque = Estoque()

@router.get("/")
def default():
    return {"message": "Estoque"}

@router.get("/integrar")
def integrar_produtos_olist():    
    return asyncio.run(estoque.atualizar_olist())