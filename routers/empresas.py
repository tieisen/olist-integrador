from fastapi import APIRouter, HTTPException, status
from src.sankhya.empresa import Empresa
import asyncio

router = APIRouter()
empresa = Empresa()

@router.get("/")
def default():
    return {"message": "Empresas"}

@router.get("/buscar")
def buscar_todas():
    """
    Busca todas as empresas
    """
    if not asyncio.run(empresa.buscar()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao buscar lista de empresas")
    return True

@router.get("/buscar/{codemp}")
def buscar_codemp(codemp:int):
    """
    Busca empresa
    """
    if not asyncio.run(empresa.buscar(codemp=codemp)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao buscar dados da empresa {codemp}")
    return True