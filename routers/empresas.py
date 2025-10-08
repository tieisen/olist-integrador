from fastapi import APIRouter, HTTPException, status
from src.sankhya.empresa import Empresa
from database.schemas import EmpresaCreate, EmpresaDB
from database.crud import empresa as crud
import asyncio

router = APIRouter()
empresa = Empresa()

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

@router.post("")
async def criar(empresa: EmpresaCreate) -> EmpresaDB:
    """
    Cadastra empresa
    """
    try:
        sucesso = await crud.criar(**empresa.model_dump())
        if not sucesso:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Erro ao criar empresa"
            )
        return True
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )