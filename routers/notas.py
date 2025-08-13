from fastapi import APIRouter
from src.integrador.nota import Nota
from src.integrador.separacao import Separacao
import asyncio

router = APIRouter()
nota = Nota()
separacao = Separacao()

@router.get("/")
def default():
    return {"message": "Notas"}

@router.get("/integrar")
def integrar_notas_olist():
    asyncio.run(nota.emitir())
    asyncio.run(separacao.checkout())
    asyncio.run(nota.confirmar())
    asyncio.run(nota.baixar_financeiro())
    return True

@router.get("/emitir")
def emitir_notas_olist():
    asyncio.run(nota.emitir())
    return True

@router.get("/confirmar")
def confirmar_notas_olist():
    asyncio.run(nota.confirmar())
    asyncio.run(nota.baixar_financeiro())
    return True

@router.get("/baixar-financeiro")
def baixar_notas_olist():
    asyncio.run(nota.baixar_financeiro())
    return True