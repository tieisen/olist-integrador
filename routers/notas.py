from fastapi import APIRouter
from src.integrador.nota import Nota
import asyncio

router = APIRouter()

@router.get("/")
def default():
    return {"message": "Notas"}

@router.get("/integrar")
def integrar_notas_olist():
    nota = Nota()
    asyncio.run(nota.emitir_notas())
    asyncio.run(nota.confirmar_notas())
    asyncio.run(nota.baixar_financeiro())
    return True

@router.get("/emitir")
def emitir_notas_olist():
    nota = Nota()
    asyncio.run(nota.emitir_notas())
    return True

@router.get("/confirmar")
def confirmar_notas_olist():
    nota = Nota()
    asyncio.run(nota.confirmar_notas())
    asyncio.run(nota.baixar_financeiro())
    return True

@router.get("/baixar-financeiro")
def baixar_notas_olist():
    nota = Nota()
    asyncio.run(nota.baixar_financeiro())
    return True